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

-module(couch_replicator).

-export([
    replicate/2,
    ensure_rep_db_exists/0,
    replication_states/0,
    job/1,
    doc/3,
    active_doc/2,
    info_from_doc/2,
    restart_job/1
]).

-include_lib("couch/include/couch_db.hrl").
-include("couch_replicator.hrl").
-include_lib("couch_replicator/include/couch_replicator_api_wrap.hrl").
-include_lib("couch_mrview/include/couch_mrview.hrl").
-include_lib("mem3/include/mem3.hrl").

-define(DESIGN_DOC_CREATION_DELAY_MSEC, 1000).
-define(REPLICATION_STATES, [
    initializing,  % Just added to scheduler
    error,         % Could not be turned into a replication job
    running,       % Scheduled and running
    pending,       % Scheduled and waiting to run
    crashing,      % Scheduled but crashing, backed off by the scheduler
    completed,     % Non-continuous (normal) completed replication
    failed         % Terminal failure, will not be retried anymore
]).

-import(couch_util, [
    get_value/2,
    get_value/3
]).


-spec replicate({[_]}, any()) ->
    {ok, {continuous, binary()}} |
    {ok, {[_]}} |
    {ok, {cancelled, binary()}} |
    {error, any()} |
    no_return().
replicate(PostBody, #user_ctx{name = UserName}) ->
    {ok, Rep0} = couch_replicator_utils:parse_rep_doc(PostBody, UserName),
    Rep = Rep0#{?START_TIME => erlang:system_time()},
    #{?REP_ID := RepId, ?OPTIONS := Options} = Rep,
    case maps:get(<<"cancel">>, Options, false) of
        true ->
            CancelRepId = case maps:get(<<"id">>, Options, nil) of
                nil -> RepId;
                RepId2 -> RepId2
            end,
            case check_authorization(CancelRepId, UserCtx) of
                ok -> cancel_replication(CancelRepId);
                not_found -> {error, not_found}
            end;
        false ->
            check_authorization(RepId, UserCtx),
            ok = couch_replicator_scheduler:add_job(Rep),
            case maps:get(<<"continuous">>, Options, false) of
                true -> {ok, {continuous, Id}};
                false -> wait_for_result(Id)
            end
    end.


% This is called from supervisor. Must respect supervisor protocol so
% it returns `ignore`.
-spec ensure_rep_db_exists() -> ignore.
ensure_rep_db_exists() ->
    couch_jobs:set_type_timeout(?REP_DOCS, ?REP_DOCS_TIMEOUT_MSEC),
    couch_jobs:set_type_timeout(?REP_JOBS, ?REP_JOBS_TIMEOUT_MSEC),
    case config:get_boolean("replicator", "create_replicator_db", false) of
        true ->
            ok = couch_replicator_docs:ensure_rep_db_exists();
        false ->
            ok
    end,
    ignore.


-spec wait_for_result(rep_id()) ->
    {ok, {[_]}} | {error, any()}.
wait_for_result(RepId) ->
    FinishRes = case couch_jobs:subscribe(?REP_JOBS, RepId) of
        {ok, finished, JobData} ->
            {ok, JobData};
        {ok, SubId, _, _} ->
            case couch_jobs:wait(SubId, finished, infinity) of
                {?REP_JOBS, RepId, finished, JobData} -> {ok, JobData};
                timeout -> timeout
            end;
        {error, Error} ->
            {error, Error}
    end,
    case FinishRes of
       {ok, #{<<"finished_result">> := CheckpointHistory}} ->
            {ok, CheckpointHistory};
       timeout ->
            {error, timeout};
       {error, Error} ->
            {error, Error}
    end.


-spec cancel_replication(rep_id()) ->
    {ok, {cancelled, binary()}} | {error, not_found}.
cancel_replication(RepId) when is_binary(RepId) ->
    couch_log:notice("Canceling replication '~s' ...", [RepId]),
    case couch_jobs:get_job_data(undefined, ?REP_JOBS, RepId) of
        {error_not, found} ->
            {error, not_found};
        #{?REP := #{?DB_NAME := null}} ->
            couch_jobs:remove(undefined, ?REP_JOBS, RepId)
            {ok, {cancelled, ?l2b(FullRepId)}};
        #{?REP := #{}} ->
            % Job was started from a replicator doc canceling via _replicate
            % doesn't quite make sense, instead replicator should be deleted.
            {error, not_found}
    end.


-spec replication_states() -> [atom()].
replication_states() ->
    ?REPLICATION_STATES.


-spec strip_url_creds(binary() | #{}) -> binary().
strip_url_creds(Endpoint) ->
    try
        couch_replicator_docs:parse_rep_db(Endpoint, #{}, #{}) of
            #{<<"url">> := Url} ->
                iolist_to_binary(couch_util:url_strip_password(Url))
    catch
        throw:{error, local_endpoints_not_supported} ->
            Endpoint
    end.


-spec job(binary()) -> {ok, {[_]}} | {error, not_found}.
job(JobId0) when is_binary(JobId0) ->
    JobId = couch_replicator_ids:convert(JobId0),
    {Res, _Bad} = rpc:multicall(couch_replicator_scheduler, job, [JobId]),
    case [JobInfo || {ok, JobInfo} <- Res] of
        [JobInfo| _] ->
            {ok, JobInfo};
        [] ->
            {error, not_found}
    end.


-spec restart_job(binary() | list() | rep_id()) ->
    {ok, {[_]}} | {error, not_found}.
restart_job(JobId0) ->
    JobId = couch_replicator_ids:convert(JobId0),
    {Res, _} = rpc:multicall(couch_replicator_scheduler, restart_job, [JobId]),
    case [JobInfo || {ok, JobInfo} <- Res] of
        [JobInfo| _] ->
            {ok, JobInfo};
        [] ->
            {error, not_found}
    end.


-spec active_doc(binary(), binary()) -> {ok, {[_]}} | {error, not_found}.
active_doc(DbName, DocId) ->
    try
        Shards = mem3:shards(DbName),
        Live = [node() | nodes()],
        Nodes = lists:usort([N || #shard{node=N} <- Shards,
            lists:member(N, Live)]),
        Owner = mem3:owner(DbName, DocId, Nodes),
        case active_doc_rpc(DbName, DocId, [Owner]) of
            {ok, DocInfo} ->
                {ok, DocInfo};
            {error, not_found} ->
                active_doc_rpc(DbName, DocId, Nodes -- [Owner])
        end
    catch
        % Might be a local database
        error:database_does_not_exist ->
            active_doc_rpc(DbName, DocId, [node()])
    end.


-spec active_doc_rpc(binary(), binary(), [node()]) ->
    {ok, {[_]}} | {error, not_found}.
active_doc_rpc(_DbName, _DocId, []) ->
    {error, not_found};
active_doc_rpc(DbName, DocId, [Node]) when Node =:= node() ->
    couch_replicator_doc_processor:doc(DbName, DocId);
active_doc_rpc(DbName, DocId, Nodes) ->
    {Res, _Bad} = rpc:multicall(Nodes, couch_replicator_doc_processor, doc,
        [DbName, DocId]),
    case [DocInfo || {ok, DocInfo} <- Res] of
        [DocInfo | _] ->
            {ok, DocInfo};
        [] ->
            {error, not_found}
    end.


-spec doc(binary(), binary(), any()) -> {ok, {[_]}} | {error, not_found}.
doc(RepDb, DocId, UserCtx) ->
    case active_doc(RepDb, DocId) of
        {ok, DocInfo} ->
            {ok, DocInfo};
        {error, not_found} ->
            doc_from_db(RepDb, DocId, UserCtx)
    end.


-spec doc_from_db(binary(), binary(), any()) -> {ok, {[_]}} | {error, not_found}.
doc_from_db(RepDb, DocId, UserCtx) ->
    case fabric:open_doc(RepDb, DocId, [UserCtx, ejson_body]) of
        {ok, Doc} ->
            {ok, info_from_doc(RepDb, couch_doc:to_json_obj(Doc, []))};
         {not_found, _Reason} ->
            {error, not_found}
    end.


-spec info_from_doc(binary(), {[_]}) -> {[_]}.
info_from_doc(RepDb, {Props}) ->
    DocId = get_value(<<"_id">>, Props),
    Source = get_value(<<"source">>, Props),
    Target = get_value(<<"target">>, Props),
    State0 = state_atom(get_value(<<"_replication_state">>, Props, null)),
    StateTime = get_value(<<"_replication_state_time">>, Props, null),
    {State1, StateInfo, ErrorCount, StartTime} = case State0 of
        completed ->
            {InfoP} = get_value(<<"_replication_stats">>, Props, {[]}),
            case lists:keytake(<<"start_time">>, 1, InfoP) of
                {value, {_, Time}, InfoP1} ->
                    {State0, {InfoP1}, 0, Time};
                false ->
                    case lists:keytake(start_time, 1, InfoP) of
                        {value, {_, Time}, InfoP1} ->
                            {State0, {InfoP1}, 0, Time};
                        false ->
                            {State0, {InfoP}, 0, null}
                        end
            end;
        failed ->
            Info = get_value(<<"_replication_state_reason">>, Props, null),
            {State0, Info, 1, StateTime};
        _OtherState ->
            {null, null, 0, null}
    end,
    {[
        {doc_id, DocId},
        {database, RepDb},
        {id, null},
        {source, strip_url_creds(Source)},
        {target, strip_url_creds(Target)},
        {state, State1},
        {error_count, ErrorCount},
        {info, StateInfo},
        {start_time, StartTime},
        {last_updated, StateTime}
     ]}.


state_atom(<<"triggered">>) ->
    triggered;  % This handles a legacy case were document wasn't converted yet
state_atom(State) when is_binary(State) ->
    erlang:binary_to_existing_atom(State, utf8);
state_atom(State) when is_atom(State) ->
    State.


-spec check_authorization(rep_id(), #user_ctx{}) -> ok | not_found.
check_authorization(RepId, #user_ctx{name = Name} = Ctx) ->
    case couch_jobs:get_job_data(undefined, ?REP_JOBS, RePid) of
        {error_not, found} ->
            not_found;
        #{?REP := {?REP_USER := Name}} ->
            ok;
        #{} ->
            couch_httpd:verify_is_server_admin(Ctx)
    end.


-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

authorization_test_() ->
    {
        foreach,
        fun () -> ok end,
        fun (_) -> meck:unload() end,
        [
            t_admin_is_always_authorized(),
            t_username_must_match(),
            t_replication_not_found()
        ]
    }.


t_admin_is_always_authorized() ->
    ?_test(begin
        expect_rep_user_ctx(<<"someuser">>, <<"_admin">>),
        UserCtx = #user_ctx{name = <<"adm">>, roles = [<<"_admin">>]},
        ?assertEqual(ok, check_authorization(<<"RepId">>, UserCtx))
    end).


t_username_must_match() ->
     ?_test(begin
        expect_rep_user_ctx(<<"user">>, <<"somerole">>),
        UserCtx1 = #user_ctx{name = <<"user">>, roles = [<<"somerole">>]},
        ?assertEqual(ok, check_authorization(<<"RepId">>, UserCtx1)),
        UserCtx2 = #user_ctx{name = <<"other">>, roles = [<<"somerole">>]},
        ?assertThrow({unauthorized, _}, check_authorization(<<"RepId">>,
            UserCtx2))
    end).


t_replication_not_found() ->
     ?_test(begin
        meck:expect(couch_replicator_scheduler, rep_state, 1, nil),
        UserCtx1 = #user_ctx{name = <<"user">>, roles = [<<"somerole">>]},
        ?assertEqual(not_found, check_authorization(<<"RepId">>, UserCtx1)),
        UserCtx2 = #user_ctx{name = <<"adm">>, roles = [<<"_admin">>]},
        ?assertEqual(not_found, check_authorization(<<"RepId">>, UserCtx2))
    end).



strip_url_creds_test_() ->
     {
        foreach,
        fun () -> meck:expect(config, get,
            fun(_, _, Default) -> Default end)
        end,
        fun (_) -> meck:unload() end,
        [
            t_strip_http_basic_creds(),
            t_strip_http_props_creds(),
            t_strip_local_db_creds()
        ]
    }.


t_strip_local_db_creds() ->
    ?_test(?assertEqual(<<"localdb">>, strip_url_creds(<<"localdb">>))).


t_strip_http_basic_creds() ->
    ?_test(begin
        Url1 = <<"http://adm:pass@host/db">>,
        ?assertEqual(<<"http://adm:*****@host/db/">>, strip_url_creds(Url1)),
        Url2 = <<"https://adm:pass@host/db">>,
        ?assertEqual(<<"https://adm:*****@host/db/">>, strip_url_creds(Url2)),
        Url3 = <<"http://adm:pass@host:80/db">>,
        ?assertEqual(<<"http://adm:*****@host:80/db/">>, strip_url_creds(Url3)),
        Url4 = <<"http://adm:pass@host/db?a=b&c=d">>,
        ?assertEqual(<<"http://adm:*****@host/db?a=b&c=d">>,
            strip_url_creds(Url4))
    end).


t_strip_http_props_creds() ->
    ?_test(begin
        Props1 = {[{<<"url">>, <<"http://adm:pass@host/db">>}]},
        ?assertEqual(<<"http://adm:*****@host/db/">>, strip_url_creds(Props1)),
        Props2 = {[ {<<"url">>, <<"http://host/db">>},
            {<<"headers">>, {[{<<"Authorization">>, <<"Basic pa55">>}]}}
        ]},
        ?assertEqual(<<"http://host/db/">>, strip_url_creds(Props2))
    end).

-endif.
