% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(couch_replicator_doc_processor).

-behaviour(gen_server).

-export([
    start_link/0
]).

-export([
   init/1,
   terminate/2,
   handle_call/3,
   handle_info/2,
   handle_cast/2,
   code_change/3
]).

-export([
    after_db_create/2,
    after_db_delete/2,
    after_doc_write/6
]).

-export([
    docs/1,
    doc/2,
    doc_lookup/3,
    update_docs/0,
    get_worker_ref/1
]).

-include_lib("couch/include/couch_db.hrl").
-include("couch_replicator.hrl").
-include_lib("mem3/include/mem3.hrl").

-import(couch_replicator_utils, [
    get_json_value/2,
    get_json_value/3
]).

-define(DEFAULT_UPDATE_DOCS, false).
-define(ERROR_MAX_BACKOFF_EXPONENT, 12).  % ~ 1 day on average
-define(TS_DAY_SEC, 86400).
-define(INITIAL_BACKOFF_EXPONENT, 64).
-define(MIN_FILTER_DELAY_SEC, 60).

-type repstate() :: initializing | error | scheduled.

-define(IS_REPLICATOR_DB(DbName), (DbName =:= ?REP_DB_NAME orelse
    binary_part(DbName, byte_size(DbName), -12) =:= <<"/_replicator">>).

-define(MAX_ACCEPTORS, 10).
-define(MAX_JOBS, 500).


% EPI db monitoring plugin callbacks

after_db_create(DbName, DbUUID) when ?IS_REPLICATOR_DB(DbName)->
    couch_stats:increment_counter([couch_replicator, docs, dbs_created]),
    add_jobs_from_db(DbName, DbUUID);

after_db_create(_DbName, _DbUUID) ->
    ok.


after_db_delete(DbName, DbUUID) when ?IS_REPLICATOR_DB(DbName) ->
    couch_stats:increment_counter([couch_replicator, docs, dbs_deleted]),
    remove_jobs_from_db(DbUUID);

after_db_delete(_DbName, _DbUUID) ->
    ok.


after_doc_write(#{name := DbName} = Db, #doc{} = Doc, _NewWinner, _OldWinner,
        _NewRevId, _Seq) when ?IS_REPLICATOR_DB(DbName) ->
    couch_stats:increment_counter([couch_replicator, docs, db_changes]),
    ok = process_change(Db, Doc);

after_doc_write(_Db, _Doc, _NewWinner, _OldWinner, _NewRevId, _Seq) ->
    ok.



% Process replication doc updates

process_change(_Db, #doc{id = <<?DESIGN_DOC_PREFIX, _/binary>>}) ->
    ok;

process_change(#{} = Db, #doc{deleted = true} = Doc) ->
    DbUUID = fabric2_db:uuid(Db),
    JobId = couch_replicator_ids:job_id(DbUUID, Doc#doc.id),
    couch_replicator_jobs:remove_job(undefined, JobId);

process_change(#{} = Db, #doc{deleted = false} = Doc) ->
    #doc{id = DocId, body = {Props} = Body} = Doc,
    DbName = fabric2_db:name(Db),
    DbUUID = fabric2_db:uuid(Db),
    {Rep, Error} = try
        Rep0 = couch_replicator_docs:parse_rep_doc(Body),
        DocState = get_json_value(?REPLICATION_STATE, Props, null),
        Rep1 = Rep0#{?DOC_STATE := DocState},
        {Rep1, null}
    catch
        % This technically shouldn't happen as we've check if documents can be
        % parsed in the BDU
        throw:{bad_rep_doc, Reason} ->
            {null, couch_replicator_utils:rep_error_to_binary(Reason)}
    end,
    JobId = couch_replicator_ids:job_id(DbUUID, DocId),
    JobData0 = #{
        ?REP => Rep,
        ?REP_ID => null,
        ?BASE_ID => null,
        ?DB_NAME => DbName,
        ?DB_UUID => DbUUID,
        ?DOC_ID => DocId,
        ?ERROR_COUNT => 0,
        ?REP_STATS => #{},
        ?LAST_UPDATED => erlang:system_time(),
        ?JOB_HISTORY => []
    },
    JobData = case Rep of
        null -> JobData0#{?STATE => ?ST_FAILED, ?STATE_INFO => Error};
        #{} -> JobData0#{?STATE => ?ST_INITIALIZING, ?STATE_INFO => null}
    end,
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(Db), fun(JTx) ->
        couch_replicate_jobs:get_job_data(JTx, JobId) of
            {ok, #{?REP := null, ?STATE_INFO := Error}} when Rep =:= null ->
                % Same error as before occurred, don't bother updating the job
                ok;
            {ok, #{?REP := null}} when Rep =:= null ->
                % Error occured but it's a different error so the job is updated
                couch_replicator_jobs:add_job(JTx, JobId, JobData);
            {ok, #{?REP := OldRep}} when is_map(Rep) ->
                case couch_replicator_utils:compare_rep_objects(OldRep, Rep) of
                    true ->
                        % Document was changed but none of the parameters relevent
                        % for the replication job have changed, so make it a no-op
                        ok;
                    false ->
                        couch_replicator_jobs:add_job(JTx, JobId, JobData)
                end;
            {error, not_found} ->
                couch_replicator_jobs:add_job(JTx, JobId, JobData)
        end

    end).


worker_fun(Job, JobData) ->
    try
        worker_fun1(Job, JobData)
    catch
        throw:halt ->
            Msg = "~p : replication doc job ~p lock conflict",
            couch_log:error(Msg, [?MODULE, Job])
    end.


worker_fun1(Job, #{?REP := null} = JobData) ->
    #{
        ?STATE_INFO := Error,
        ?DB_NAME := DbName,
        ?DOC_ID := DocId
    } = JobData,
    finish_with_permanent_failure(undefined, Job, JobData, Error),
    couch_replicator_docs:update_failed(DbName, DocId, Error);


worker_fun1(Job, #{?REP := Rep = #{}} = JobData) ->
    ok = remove_old_state_fields(JobData),
    try
        {NewRepId, NewBaseId} = couch_replicator_ids:replication_id(Rep),
        worker_fun2(Job, {NewRepId, NewBaseId}, JobData)
    catch
        throw:{filter_fetch_error, Error} ->
            Error1 = io_lib:format("Filter fetch error ~p", [Error]),
            Error2 = couch_util:to_binary(Error1),
            finish_with_temporary_error(undefined, Job, JobData, Error2),
            #{?REP_ID := RepId, ?DB_NAME := DbName, ?DOC_ID := DocId} = JobData,
            maybe_update_doc_error(OldRepId, DbName, DocId, Error2)
    end.



worker_fun2(Job, {NewRepId, NewBaseId}, #{} = JobData) ->
    #{?REP := Rep, ?REP_ID := OldRepId, ?DB_NAME := DbName, ?DOC_ID := DocId} = JobData,
    Result = couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(Tx), fun(JTx) ->
        % Clear old repid -> job_id reference
        case couch_replication_jobs:update_replication_id(JTx, JobId, RepId) of
            ok ->
                JobData1 = JobData#{?REP_ID := NewRepId, ?BASE_ID := NewBaseId},
                maybe_start_replication_job(JTx, Job, JobData1);
            {error, {replication_job_conflict, OtherJobId}} ->
                % TODO check other job data, either fail or reschedule
                % to try later. If other job is transient, maybe stop the other
                % job and start this one
                {error, {temporary_error, RepId, Error}}

    end),
    case Result of
        {ok, RepId} ->
            maybe_update_doc_triggered(DbName, DocId, RepId);
        ignore ->
            ok;
        {error, {permanent_failure, Error}}  ->
            couch_replicator_docs:update_failed(DbName, DocId, Error);
        {error, {temporary_error, RepId, Error}} ->
            maybe_update_doc_error(RepId, DbName, DocId, Error)
    end.


maybe_start_replication_job(JTx, Job, #{} = JobData) ->
    #{?REP := Rep, ?REP_ID := RepId, ?DB_UUID := DbUUID, ?DOC_ID := DocId} = JobData,
    case couch_replicator_jobs:get_job_data(JTx, RepId) of
        {error, not_found} ->
            start_replication_job(JTx, Job, Rep, JobData);
        {ok, #{?REP := {?DB_UUID := DbUUID, ?DOC_ID := DocId}} = CurRep} ->
            case couch_replicator_utils:compare_rep_objects(Rep, CurRep) of
                true ->
                    dont_start_replication_job(JTx, Job, Rep, JobData);
                false ->
                    ok = couch_replicator_jobs:remove_job(JTx, RepId),
                    start_replication_job(JTx, Job, Rep, JobData)
            end;
        {ok, #{?DB_NAME := null}} ->
            Err1 = io_lib:format("Replication `~s` specified by `~s:~s`"
                " already running as a transient replication, started via"
                " `_replicate` API endpoint", [RepId, DbName, DocId]),
            Err2 = couch_util:to_binary(Err1),
            ok = finish_with_temporary_error(JTx, Job, JobData, Err2),
            {error, {temporary_error, RepId, Error2}};
        {ok, #{?DB_NAME := OtherDb, ?DOC_ID := OtherDoc}} ->
            Err1 = io_lib:format("Replication `~s` specified by `~s:~s`"
                " already started by document `~s:~s`", [RepId, DocId,
                DbName, OtherDb, OtherDoc],
            Error2 = couch_util:to_binary(Err1),
            ok = finish_with_permanent_failure(JTx, Job, JobData, Error),
            {error, {permanent_failure, Error2}}
    end.


finish_with_temporary_error(JTx, Job, JobData, Error) ->
    #{?ERROR_COUNT := ErrorCount} = JobData,
    ErrorCount1 = ErrorCount + 1,
    JobData1 = JobData#{
        ?STATE := ?ST_ERROR,
        ?STATE_INFO := Error,
        ?ERROR_COUNT := ErrorCount1,
    } = JobData,
    schedule_error_backoff(JTx, Job, ErrorCount1),
    case couch_jobs:finish(JTx, Job, JobData1) of
        ok -> ok;
        {error, halt} -> throw(halt)
    end.


finish_with_permanent_failure(JTx, Job, JobData, Error) ->
    #{?ERROR_COUNT := ErrorCount} = JobData,
    JobData1 = JobData#{
        ?STATE := ?ST_FAILED,
        ?STATE_INFO := Error,
        ?ERROR_COUNT := ErrorCount + 1,
    } = JobData,
    case couch_jobs:finish(JTx, Job, JobData1) of
        ok -> ok;
        {error, halt} -> throw(halt)
    end.


dont_start_replication_job(JTx, Job, JobData) ->
    JobData1 = JobData#{?LAST_UPDATED => erlang:system_time()},
    ok = schedule_filter_check(JTx, Job, JobData1),
    case couch_jobs:finish(JTx, Job, JobData1) of
        ok -> ignore;
        {error, halt} -> throw(halt)
    end.


start_replication_job(JTx, Job, #{} = JobData) ->
    #{?REP_ID := RepId} = JobData,
    JobData1 = JobData#{
        ?STATE => ?ST_PENDING,
        ?STATE_INFO => null,
        ?ERROR_COUNT => 0,
        ?LAST_UPDATED => erlang:system_time(),
        ?HISTORY => [] % Todo: update history
    },
    ok = couch_replicator_jobs:add_job(JTx, RepId, JobData1),
    ok = schedule_filter_check(JTx, Job, JobData1),
    case couch_jobs:finish(JTx, Job, JobData1) of
        ok -> {ok, RepId};
        {error, halt} -> throw(halt)
    end.


schedule_error_backoff(JTx, Job, ErrorCount) ->
    Exp = min(ErrCnt, ?ERROR_MAX_BACKOFF_EXPONENT),
    % ErrCnt is the exponent here. The reason 64 is used is to start at
    % 64 (about a minute) max range. Then first backoff would be 30 sec
    % on average. Then 1 minute and so on.
    NowSec = erlang:system_time(second),
    When = NowSec + rand:uniform(?INITIAL_BACKOFF_EXPONENT bsl Exp).
    couch_jobs:resubmit(JTx, Job, trunc(When)).


schedule_filter_check(JTx, Job, #{} = JobData) ->
    #{?REP := Rep} = JobData,
    #{?OPTIONS := Opts} = Rep,
    case couch_replicator_filter:parse(Opts) of
        {ok, {user, _FName, _QP}} ->
            % For user filters, we have to periodically check the source
            % in case the filter defintion has changed
            IntervalSec = filter_check_interval_sec(),
            NowSec = erlang:system_time(second),
            When = NowSec + 0.5 * IntervalSec + rand:uniform(IntervalSec),
            couch_jobs:resubmit(JTx, Job, trunc(When));
        _ ->
            ok
    end.

remove_old_state_fields(#{?DOC_STATE := DocState} = JobData) when
        DocState =:= ?TRIGGERED orelse DocState =:= ?ERROR ->
    case update_docs() of
        true ->
            ok;
        false ->
            #{?REP := Rep} = JobData,
            #{?DB_NAME := DbName, ?DOC_ID := DocId} = Rep,
            couch_replicator_docs:remove_state_fields(DbName, DocId)
    end;

remove_old_state_fields(#{}) ->
    ok.


-spec maybe_update_doc_error(binary(), binary(), binary(), any()) -> ok.
maybe_update_doc_error(RepId, DbName, DocId, Error) ->
    case update_docs() of
        true ->
            couch_replicator_docs:update_error(RepId, DbName, DocId, Error);
        false ->
            ok
    end.


-spec maybe_update_doc_triggered(#{}, rep_id()) -> ok.
maybe_update_doc_triggered(RepId, DbName, DocId) ->
    case update_docs() of
        true ->
            couch_replicator_docs:update_triggered(RepId, DbName, DocId);
        false ->
            ok
    end.


-spec error_backoff(non_neg_integer()) -> seconds().
error_backoff(ErrCnt) ->
    Exp = min(ErrCnt, ?ERROR_MAX_BACKOFF_EXPONENT),
    % ErrCnt is the exponent here. The reason 64 is used is to start at
    % 64 (about a minute) max range. Then first backoff would be 30 sec
    % on average. Then 1 minute and so on.
    couch_rand:uniform(?INITIAL_BACKOFF_EXPONENT bsl Exp).


-spec update_docs() -> boolean().
update_docs() ->
    config:get_boolean("replicator", "update_docs", ?DEFAULT_UPDATE_DOCS).


-spec filter_check_interval_sec() -> integer().
filter_check_interval_sec() ->
    config:get_integer("replicator", "filter_check_interval_sec",
        ?DEFAULT_FILTER_CHECK_INTERVAL_SEC).


% _scheduler/docs HTTP endpoint helpers

-spec docs([atom()]) -> [{[_]}] | [].
docs(States) ->
    HealthThreshold = couch_replicator_scheduler:health_threshold(),
    ets:foldl(fun(RDoc, Acc) ->
        case ejson_doc(RDoc, HealthThreshold) of
            nil ->
                Acc;  % Could have been deleted if job just completed
            {Props} = EJson ->
                {state, DocState} = lists:keyfind(state, 1, Props),
                case ejson_doc_state_filter(DocState, States) of
                    true ->
                        [EJson | Acc];
                    false ->
                        Acc
                end
        end
    end, [], ?MODULE).


-spec doc(binary(), binary()) -> {ok, {[_]}} | {error, not_found}.
doc(Db, DocId) ->
    HealthThreshold = couch_replicator_scheduler:health_threshold(),
    Res = (catch ets:foldl(fun(RDoc, nil) ->
        {Shard, RDocId} = RDoc#rdoc.id,
        case {mem3:dbname(Shard), RDocId} of
            {Db, DocId} ->
                throw({found, ejson_doc(RDoc, HealthThreshold)});
            {_OtherDb, _OtherDocId} ->
                nil
        end
    end, nil, ?MODULE)),
    case Res of
        {found, DocInfo} ->
            {ok, DocInfo};
        nil ->
            {error, not_found}
    end.


-spec doc_lookup(binary(), binary(), integer()) ->
    {ok, {[_]}} | {error, not_found}.
doc_lookup(Db, DocId, HealthThreshold) ->
    case ets:lookup(?MODULE, {Db, DocId}) of
        [#rdoc{} = RDoc] ->
            {ok, ejson_doc(RDoc, HealthThreshold)};
        [] ->
            {error, not_found}
    end.


-spec ejson_state_info(binary() | nil) -> binary() | null.
ejson_state_info(nil) ->
    null;
ejson_state_info(Info) when is_binary(Info) ->
    Info;
ejson_state_info(Info) ->
    couch_replicator_utils:rep_error_to_binary(Info).


-spec ejson_rep_id(rep_id() | nil) -> binary() | null.
ejson_rep_id(nil) ->
    null;
ejson_rep_id({BaseId, Ext}) ->
    iolist_to_binary([BaseId, Ext]).


-spec ejson_doc(#rdoc{}, non_neg_integer()) -> {[_]} | nil.
ejson_doc(#rdoc{state = scheduled} = RDoc, HealthThreshold) ->
    #rdoc{id = {DbName, DocId}, rid = RepId} = RDoc,
    JobProps = couch_replicator_scheduler:job_summary(RepId, HealthThreshold),
    case JobProps of
        nil ->
            nil;
        [{_, _} | _] ->
            {[
                {doc_id, DocId},
                {database, DbName},
                {id, ejson_rep_id(RepId)},
                {node, node()} | JobProps
            ]}
    end;

ejson_doc(#rdoc{state = RepState} = RDoc, _HealthThreshold) ->
    #rdoc{
       id = {DbName, DocId},
       info = StateInfo,
       rid = RepId,
       errcnt = ErrorCount,
       last_updated = StateTime,
       rep = Rep
    } = RDoc,
    {[
        {doc_id, DocId},
        {database, DbName},
        {id, ejson_rep_id(RepId)},
        {state, RepState},
        {info, ejson_state_info(StateInfo)},
        {error_count, ErrorCount},
        {node, node()},
        {last_updated, couch_replicator_utils:iso8601(StateTime)},
        {start_time, couch_replicator_utils:iso8601(Rep#rep.start_time)}
    ]}.


-spec ejson_doc_state_filter(atom(), [atom()]) -> boolean().
ejson_doc_state_filter(_DocState, []) ->
    true;
ejson_doc_state_filter(State, States) when is_list(States), is_atom(State) ->
    lists:member(State, States).


-spec remove_jobs_from_db(binary()) -> ok.
remove_jobs_from_db(DbUUID) when is_binary(DbUUID) ->
    FoldFun = fun({JTx, JobId, _, JobData}, ok) ->
        case JobData of
            #{?DB_UUID := DbUUID} ->
                ok = couch_replicator_jobs:remove_job(JTx, JobId);
            #{} ->
                ok
        end
    end,
    couch_replicator_jobs:fold_jobs(undefined, FoldFun, ok).


-spec add_jobs_from_db(binary(), binary())-> ok.
add_jobs_from_db(DbName, DbUUID) when is_binary(DbUUID) ->
    try fabric2_db:open(DbName, [{uuid, DbUUID}]) of
        {ok, Db} ->
            fabric2_fdb:transactional(Db, fun(TxDb) ->
                ok = add_jobs_from_db(TxDb)
            end)
    catch
        error:database_does_not_exist ->
            ok
    end.


-spec add_jobs_from_db(#{}) -> ok.
add_jobs_from_db(#{} = TxDb) ->
    FoldFun  = fun
        ({meta, _Meta}, ok) -> {ok, ok};
        (complete, ok) -> {ok, ok};
        ({row, Row}, ok) -> ok = process_change(TxDb, get_doc(TxDb, Row))
    end,
    Opts = [{restart_tx, true}],
    {ok, ok} = fabric2_db:fold_docs(Db, FoldFun, ok, Opts),
    ok.


-spec get_doc(#{}, list()) -> #doc{}.
get_doc(Db, Row) ->
    {_, DocId} = lists:keyfind(id, 1, Row),
    {ok, #doc{deleted = false} = Doc} = fabric2_db:open_doc(TxDb, DocId, []),
    Doc.


-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-define(DB, <<"db">>).
-define(DOC1, <<"doc1">>).
-define(DOC2, <<"doc2">>).
-define(R1, {"1", ""}).
-define(R2, {"2", ""}).


doc_processor_test_() ->
    {
        foreach,
        fun setup/0,
        fun teardown/1,
        [
            t_bad_change(),
            t_regular_change(),
            t_change_with_existing_job(),
            t_deleted_change(),
            t_triggered_change(),
            t_completed_change(),
            t_active_replication_completed(),
            t_error_change(),
            t_failed_change(),
            t_change_for_different_node(),
            t_change_when_cluster_unstable(),
            t_ejson_docs()
        ]
    }.


% Can't parse replication doc, so should write failure state to document.
t_bad_change() ->
    ?_test(begin
        ?assertEqual(acc, db_change(?DB, bad_change(), acc)),
        ?assert(updated_doc_with_failed_state())
    end).


% Regular change, parse to a #rep{} and then add job.
t_regular_change() ->
    ?_test(begin
        mock_existing_jobs_lookup([]),
        ?assertEqual(ok, process_change(?DB, change())),
        ?assert(ets:member(?MODULE, {?DB, ?DOC1})),
        ?assert(started_worker({?DB, ?DOC1}))
    end).


% Regular change, parse to a #rep{} and then add job but there is already
% a running job with same Id found.
t_change_with_existing_job() ->
    ?_test(begin
        mock_existing_jobs_lookup([test_rep(?R2)]),
        ?assertEqual(ok, process_change(?DB, change())),
        ?assert(ets:member(?MODULE, {?DB, ?DOC1})),
        ?assert(started_worker({?DB, ?DOC1}))
    end).


% Change is a deletion, and job is running, so remove job.
t_deleted_change() ->
    ?_test(begin
        mock_existing_jobs_lookup([test_rep(?R2)]),
        ?assertEqual(ok, process_change(?DB, deleted_change())),
        ?assert(removed_job(?R2))
    end).


% Change is in `triggered` state. Remove legacy state and add job.
t_triggered_change() ->
    ?_test(begin
        mock_existing_jobs_lookup([]),
        ?assertEqual(ok, process_change(?DB, change(<<"triggered">>))),
        ?assert(removed_state_fields()),
        ?assert(ets:member(?MODULE, {?DB, ?DOC1})),
        ?assert(started_worker({?DB, ?DOC1}))
    end).


% Change is in `completed` state, so skip over it.
t_completed_change() ->
    ?_test(begin
        ?assertEqual(ok, process_change(?DB, change(<<"completed">>))),
        ?assert(did_not_remove_state_fields()),
        ?assertNot(ets:member(?MODULE, {?DB, ?DOC1})),
        ?assert(did_not_spawn_worker())
    end).


% Completed change comes for what used to be an active job. In this case
% remove entry from doc_processor's ets (because there is no linkage or
% callback mechanism for scheduler to tell doc_processsor a replication just
% completed).
t_active_replication_completed() ->
    ?_test(begin
        mock_existing_jobs_lookup([]),
        ?assertEqual(ok, process_change(?DB, change())),
        ?assert(ets:member(?MODULE, {?DB, ?DOC1})),
        ?assertEqual(ok, process_change(?DB, change(<<"completed">>))),
        ?assert(did_not_remove_state_fields()),
        ?assertNot(ets:member(?MODULE, {?DB, ?DOC1}))
    end).


% Change is in `error` state. Remove legacy state and retry
% running the job. This state was used for transient erorrs which are not
% written to the document anymore.
t_error_change() ->
    ?_test(begin
        mock_existing_jobs_lookup([]),
        ?assertEqual(ok, process_change(?DB, change(<<"error">>))),
        ?assert(removed_state_fields()),
        ?assert(ets:member(?MODULE, {?DB, ?DOC1})),
        ?assert(started_worker({?DB, ?DOC1}))
    end).


% Change is in `failed` state. This is a terminal state and it will not
% be tried again, so skip over it.
t_failed_change() ->
    ?_test(begin
        ?assertEqual(ok, process_change(?DB, change(<<"failed">>))),
        ?assert(did_not_remove_state_fields()),
        ?assertNot(ets:member(?MODULE, {?DB, ?DOC1})),
        ?assert(did_not_spawn_worker())
    end).


% Normal change, but according to cluster ownership algorithm, replication
% belongs to a different node, so this node should skip it.
t_change_for_different_node() ->
   ?_test(begin
        meck:expect(couch_replicator_clustering, owner, 2, different_node),
        ?assertEqual(ok, process_change(?DB, change())),
        ?assert(did_not_spawn_worker())
   end).


% Change handled when cluster is unstable (nodes are added or removed), so
% job is not added. A rescan will be triggered soon and change will be
% evaluated again.
t_change_when_cluster_unstable() ->
   ?_test(begin
       meck:expect(couch_replicator_clustering, owner, 2, unstable),
       ?assertEqual(ok, process_change(?DB, change())),
       ?assert(did_not_spawn_worker())
   end).


% Check if docs/0 function produces expected ejson after adding a job
t_ejson_docs() ->
    ?_test(begin
        mock_existing_jobs_lookup([]),
        ?assertEqual(ok, process_change(?DB, change())),
        ?assert(ets:member(?MODULE, {?DB, ?DOC1})),
        EJsonDocs = docs([]),
        ?assertMatch([{[_|_]}], EJsonDocs),
        [{DocProps}] = EJsonDocs,
        {value, StateTime, DocProps1} = lists:keytake(last_updated, 1,
            DocProps),
        ?assertMatch({last_updated, BinVal1} when is_binary(BinVal1),
            StateTime),
        {value, StartTime, DocProps2} = lists:keytake(start_time, 1, DocProps1),
        ?assertMatch({start_time, BinVal2} when is_binary(BinVal2), StartTime),
        ExpectedProps = [
            {database, ?DB},
            {doc_id, ?DOC1},
            {error_count, 0},
            {id, null},
            {info, null},
            {node, node()},
            {state, initializing}
        ],
        ?assertEqual(ExpectedProps, lists:usort(DocProps2))
    end).


get_worker_ref_test_() ->
    {
        setup,
        fun() ->
            ets:new(?MODULE, [named_table, public, {keypos, #rdoc.id}])
        end,
        fun(_) -> ets:delete(?MODULE) end,
        ?_test(begin
            Id = {<<"db">>, <<"doc">>},
            ?assertEqual(nil, get_worker_ref(Id)),
            ets:insert(?MODULE, #rdoc{id = Id, worker = nil}),
            ?assertEqual(nil, get_worker_ref(Id)),
            Ref = make_ref(),
            ets:insert(?MODULE, #rdoc{id = Id, worker = Ref}),
            ?assertEqual(Ref, get_worker_ref(Id))
        end)
    }.


% Test helper functions


setup() ->
    meck:expect(couch_log, info, 2, ok),
    meck:expect(couch_log, notice, 2, ok),
    meck:expect(couch_log, warning, 2, ok),
    meck:expect(couch_log, error, 2, ok),
    meck:expect(config, get, fun(_, _, Default) -> Default end),
    meck:expect(config, listen_for_changes, 2, ok),
    meck:expect(couch_replicator_clustering, owner, 2, node()),
    meck:expect(couch_replicator_clustering, link_cluster_event_listener, 3,
        ok),
    meck:expect(couch_replicator_doc_processor_worker, spawn_worker, 4, pid),
    meck:expect(couch_replicator_scheduler, remove_job, 1, ok),
    meck:expect(couch_replicator_docs, remove_state_fields, 2, ok),
    meck:expect(couch_replicator_docs, update_failed, 3, ok),
    {ok, Pid} = start_link(),
    Pid.


teardown(Pid) ->
    unlink(Pid),
    exit(Pid, kill),
    meck:unload().


removed_state_fields() ->
    meck:called(couch_replicator_docs, remove_state_fields, [?DB, ?DOC1]).


started_worker(_Id) ->
    1 == meck:num_calls(couch_replicator_doc_processor_worker, spawn_worker, 4).


removed_job(Id) ->
    meck:called(couch_replicator_scheduler, remove_job, [test_rep(Id)]).


did_not_remove_state_fields() ->
    0 == meck:num_calls(couch_replicator_docs, remove_state_fields, '_').


did_not_spawn_worker() ->
    0 == meck:num_calls(couch_replicator_doc_processor_worker, spawn_worker,
        '_').

updated_doc_with_failed_state() ->
    1 == meck:num_calls(couch_replicator_docs, update_failed, '_').


mock_existing_jobs_lookup(ExistingJobs) ->
    meck:expect(couch_replicator_scheduler, find_jobs_by_doc,
        fun(?DB, ?DOC1) -> ExistingJobs end).


test_rep(Id) ->
  #rep{id = Id, start_time = {0, 0, 0}}.


change() ->
    {[
        {?REP_ID, ?DOC1},
        {doc, {[
            {<<"_id">>, ?DOC1},
            {<<"source">>, <<"http://srchost.local/src">>},
            {<<"target">>, <<"http://tgthost.local/tgt">>}
        ]}}
    ]}.


change(State) ->
    {[
        {?REP_ID, ?DOC1},
        {doc, {[
            {<<"_id">>, ?DOC1},
            {<<"source">>, <<"http://srchost.local/src">>},
            {<<"target">>, <<"http://tgthost.local/tgt">>},
            {<<"_replication_state">>, State}
        ]}}
    ]}.


deleted_change() ->
    {[
        {?REP_ID, ?DOC1},
        {<<"deleted">>, true},
        {doc, {[
            {<<"_id">>, ?DOC1},
            {<<"source">>, <<"http://srchost.local/src">>},
            {<<"target">>, <<"http://tgthost.local/tgt">>}
        ]}}
    ]}.


bad_change() ->
    {[
        {?REP_ID, ?DOC2},
        {doc, {[
            {<<"_id">>, ?DOC2},
            {<<"source">>, <<"src">>}
        ]}}
    ]}.

-endif.
