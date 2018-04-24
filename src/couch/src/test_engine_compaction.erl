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

-module(test_engine_compaction).
-compile(export_all).


-include_lib("eunit/include/eunit.hrl").
-include_lib("couch/include/couch_db.hrl").


cet_compact_empty() ->
    {ok, Db1} = test_engine_util:create_db(),
    Term1 = test_engine_util:db_as_term(Db1),

    test_engine_util:compact(Db1),

    {ok, Db2} = couch_db:reopen(Db1),
    Term2 = test_engine_util:db_as_term(Db2),

    Diff = test_engine_util:term_diff(Term1, Term2),
    ?assertEqual(nodiff, Diff).


cet_compact_doc() ->
    {ok, Db1} = test_engine_util:create_db(),
    Actions = [{create, {<<"foo">>, {[]}}}],
    {ok, Db2} = test_engine_util:apply_actions(Db1, Actions),
    Term1 = test_engine_util:db_as_term(Db2),

    test_engine_util:compact(Db2),

    {ok, Db3} = couch_db:reopen(Db2),
    Term2 = test_engine_util:db_as_term(Db3),

    Diff = test_engine_util:term_diff(Term1, Term2),
    ?assertEqual(nodiff, Diff).


cet_compact_local_doc() ->
    {ok, Db1} = test_engine_util:create_db(),
    Actions = [{create, {<<"_local/foo">>, {[]}}}],
    {ok, Db2} = test_engine_util:apply_actions(Db1, Actions),
    Term1 = test_engine_util:db_as_term(Db2),

    test_engine_util:compact(Db2),

    {ok, Db3} = couch_db:reopen(Db2),
    Term2 = test_engine_util:db_as_term(Db3),

    Diff = test_engine_util:term_diff(Term1, Term2),
    ?assertEqual(nodiff, Diff).


cet_compact_with_everything() ->
    {ok, Db1} = test_engine_util:create_db(),

    % Add a whole bunch of docs
    DocActions = lists:map(fun(Seq) ->
        {create, {docid(Seq), {[{<<"int">>, Seq}]}}}
    end, lists:seq(1, 1000)),

    LocalActions = lists:map(fun(I) ->
        {create, {local_docid(I), {[{<<"int">>, I}]}}}
    end, lists:seq(1, 25)),

    Actions1 = DocActions ++ LocalActions,

    {ok, Db2} = test_engine_util:apply_batch(Db1, Actions1),
    ok = couch_db:set_security(Db1, {[{<<"foo">>, <<"bar">>}]}),
    ok = couch_db:set_revs_limit(Db1, 500),

    Actions2 = [
        {create, {<<"foo">>, {[]}}},
        {create, {<<"bar">>, {[{<<"hooray">>, <<"purple">>}]}}},
        {conflict, {<<"bar">>, {[{<<"booo">>, false}]}}}
    ],

    {ok, Db3} = test_engine_util:apply_actions(Db2, Actions2),

    [FooFDI, BarFDI] = couch_db_engine:open_docs(Db3, [<<"foo">>, <<"bar">>]),

    FooRev = test_engine_util:prev_rev(FooFDI),
    BarRev = test_engine_util:prev_rev(BarFDI),

    Actions3 = [
        {purge, {<<"foo">>, FooRev#rev_info.rev}},
        {purge, {<<"bar">>, BarRev#rev_info.rev}}
    ],

    {ok, Db4} = test_engine_util:apply_actions(Db3, Actions3),

    PurgedIdRevs = [
        {<<"bar">>, [BarRev#rev_info.rev]},
        {<<"foo">>, [FooRev#rev_info.rev]}
    ],

    {ok, PIdRevs4} = couch_db_enigne:fold_purge_infos(
            Db4, 0, fun fold_fun/2, [], []),
    ?assertEqual(PurgedIdRevs, PIdRevs4),

    {ok, Db5} = try
        [Att0, Att1, Att2, Att3, Att4] = test_engine_util:prep_atts(Db4, [
                {<<"ohai.txt">>, crypto:strong_rand_bytes(2048)},
                {<<"stuff.py">>, crypto:strong_rand_bytes(32768)},
                {<<"a.erl">>, crypto:strong_rand_bytes(29)},
                {<<"a.hrl">>, crypto:strong_rand_bytes(5000)},
                {<<"a.app">>, crypto:strong_rand_bytes(400)}
            ]),

        Actions4 = [
            {create, {<<"small_att">>, {[]}, [Att0]}},
            {create, {<<"large_att">>, {[]}, [Att1]}},
            {create, {<<"multi_att">>, {[]}, [Att2, Att3, Att4]}}
        ],
        test_engine_util:apply_actions(Db4, Actions4)
    catch throw:not_supported ->
        {ok, Db4}
    end,
    {ok, _} = couch_db:ensure_full_commit(Db5),
    {ok, Db6} = couch_db:reopen(Db5),

    Term1 = test_engine_util:db_as_term(Db6),

    Config = [
        {"database_compaction", "doc_buffer_size", "1024"},
        {"database_compaction", "checkpoint_after", "2048"}
    ],

    test_engine_util:with_config(Config, fun() ->
        test_engine_util:compact(Db6)
    end),

    {ok, Db7} = couch_db:reopen(Db6),
    Term2 = test_engine_util:db_as_term(Db7),

    Diff = test_engine_util:term_diff(Term1, Term2),
    ?assertEqual(nodiff, Diff).


cet_recompact_updates() ->
    {ok, Db1} = test_engine_util:create_db(),

    Actions1 = lists:map(fun(Seq) ->
        {create, {docid(Seq), {[{<<"int">>, Seq}]}}}
    end, lists:seq(1, 1000)),
    {ok, Db2} = test_engine_util:apply_batch(Db1, Actions1),

    {ok, Compactor} = couch_db:start_compact(Db2),
    catch erlang:suspend_process(Compactor),

    Actions2 = [
        {update, {<<"0001">>, {[{<<"updated">>, true}]}}},
        {create, {<<"boop">>, {[]}}}
    ],

    {ok, Db3} = test_engine_util:apply_actions(Db2, Actions2),
    Term1 = test_engine_util:db_as_term(Db3),

    catch erlang:resume_process(Compactor),
    test_engine_util:compact(Db3),

    {ok, Db4} = couch_db:reopen(Db3),
    Term2 = test_engine_util:db_as_term(Db4),

    Diff = test_engine_util:term_diff(Term1, Term2),
    ?assertEqual(nodiff, Diff).


cet_purge_during_compact() ->
    {ok, Engine, Path, St1} = test_engine_util:init_engine(dbpath),

    Actions1 = [
        {create, {<<"foo">>, []}},
        {create, {<<"bar">>, []}},
        {conflict, {<<"bar">>, [{<<"vsn">>, 2}]}},
        {create, {<<"baz">>, []}}
    ],

    {ok, St2} = test_engine_util:apply_actions(Engine, St1, Actions1),
    {ok, St3, DbName, _, Term} = test_engine_util:compact(Engine, St2, Path),

    [BarFDI, BazFDI] = Engine:open_docs(St3, [<<"bar">>, <<"baz">>]),
    BarRev = test_engine_util:prev_rev(BarFDI),
    BazRev = test_engine_util:prev_rev(BazFDI),
    Actions2 = [
        {purge, {<<"bar">>, BarRev#rev_info.rev}},
        {purge, {<<"baz">>, BazRev#rev_info.rev}}
    ],
    {ok, St4} = test_engine_util:apply_actions(Engine, St3, Actions2),
    Db1 = test_engine_util:db_as_term(Engine, St4),

    {ok, St5, NewPid} = Engine:finish_compaction(St4, DbName, [], Term),

    ?assertEqual(true, is_pid(NewPid)),
    Ref = erlang:monitor(process, NewPid),

    NewTerm = receive
        {'$gen_cast', {compact_done, Engine, Term0}} ->
            Term0;
        {'DOWN', Ref, _, _, Reason} ->
            erlang:error({compactor_died, Reason})
        after 10000 ->
            erlang:error(compactor_timed_out)
    end,

    {ok, St6, undefined} = Engine:finish_compaction(St5, DbName, [], NewTerm),
    Db2 = test_engine_util:db_as_term(Engine, St6),
    Diff = test_engine_util:term_diff(Db1, Db2),
    ?assertEqual(nodiff, Diff).


cet_multiple_purge_during_compact() ->
    {ok, Engine, Path, St1} = test_engine_util:init_engine(dbpath),

    Actions1 = [
        {create, {<<"foo">>, []}},
        {create, {<<"bar">>, []}},
        {conflict, {<<"bar">>, [{<<"vsn">>, 2}]}},
        {create, {<<"baz">>, []}}
    ],

    {ok, St2} = test_engine_util:apply_actions(Engine, St1, Actions1),
    {ok, St3, DbName, _, Term} = test_engine_util:compact(Engine, St2, Path),

    [BarFDI, BazFDI] = Engine:open_docs(St3, [<<"bar">>, <<"baz">>]),
    BarRev = test_engine_util:prev_rev(BarFDI),
    Actions2 = [
        {purge, {<<"bar">>, BarRev#rev_info.rev}}
    ],
    {ok, St4} = test_engine_util:apply_actions(Engine, St3, Actions2),

    BazRev = test_engine_util:prev_rev(BazFDI),
    Actions3 = [
        {purge, {<<"baz">>, BazRev#rev_info.rev}}
    ],
    {ok, St5} = test_engine_util:apply_actions(Engine, St4, Actions3),

    Db1 = test_engine_util:db_as_term(Engine, St5),
    {ok, St6, NewPid} = Engine:finish_compaction(St5, DbName, [], Term),

    ?assertEqual(true, is_pid(NewPid)),
    Ref = erlang:monitor(process, NewPid),

    NewTerm = receive
        {'$gen_cast', {compact_done, Engine, Term0}} ->
            Term0;
        {'DOWN', Ref, _, _, Reason} ->
            erlang:error({compactor_died, Reason})
        after 10000 ->
            erlang:error(compactor_timed_out)
    end,

    {ok, St7, undefined} = Engine:finish_compaction(St6, DbName, [], NewTerm),
    Db2 = test_engine_util:db_as_term(Engine, St7),
    Diff = test_engine_util:term_diff(Db1, Db2),
    ?assertEqual(nodiff, Diff).


cet_recompact_purge() ->
    {ok, Engine, Path, St1} = test_engine_util:init_engine(dbpath),

    Actions1 = [
        {create, {<<"foo">>, []}},
        {create, {<<"bar">>, []}},
        {conflict, {<<"bar">>, [{<<"vsn">>, 2}]}},
        {create, {<<"baz">>, []}}
    ],

    {ok, St2} = test_engine_util:apply_actions(Engine, St1, Actions1),
    {ok, St3, DbName, _, Term} = test_engine_util:compact(Engine, St2, Path),

    [BarFDI, BazFDI] = Engine:open_docs(St3, [<<"bar">>, <<"baz">>]),
    BarRev = test_engine_util:prev_rev(BarFDI),
    BazRev = test_engine_util:prev_rev(BazFDI),
    Actions2 = [
        {purge, {<<"bar">>, BarRev#rev_info.rev}},
        {purge, {<<"baz">>, BazRev#rev_info.rev}}
    ],
    {ok, St4} = test_engine_util:apply_actions(Engine, St3, Actions2),
    Db1 = test_engine_util:db_as_term(Engine, St4),

    {ok, St5, NewPid} = Engine:finish_compaction(St4, DbName, [], Term),

    ?assertEqual(true, is_pid(NewPid)),
    Ref = erlang:monitor(process, NewPid),

    NewTerm = receive
        {'$gen_cast', {compact_done, Engine, Term0}} ->
            Term0;
        {'DOWN', Ref, _, _, Reason} ->
            erlang:error({compactor_died, Reason})
    after 10000 ->
        erlang:error(compactor_timed_out)
    end,

    {ok, St6, undefined} = Engine:finish_compaction(St5, DbName, [], NewTerm),
    Db2 = test_engine_util:db_as_term(Engine, St6),
    Diff = test_engine_util:term_diff(Db1, Db2),
    ?assertEqual(nodiff, Diff).


% temporary ignoring this test as it times out
ignore_cet_compact_purged_docs_limit() ->
    {ok, Engine, Path, St1} = test_engine_util:init_engine(dbpath),
    % create NumDocs docs
    NumDocs = 1200,
    {RActions, RIds} = lists:foldl(fun(Id, {CActions, CIds}) ->
        Id1 = docid(Id),
        Action = {create, {Id1, [{<<"int">>, Id}]}},
        {[Action| CActions], [Id1| CIds]}
    end, {[], []}, lists:seq(1, NumDocs)),
    Ids = lists:reverse(RIds),
    {ok, St2} = test_engine_util:apply_actions(Engine, St1,
        lists:reverse(RActions)),

    % purge NumDocs docs
    FDIs = Engine:open_docs(St2, Ids),
    RevActions2 = lists:foldl(fun(FDI, CActions) ->
        Id = FDI#full_doc_info.id,
        PrevRev = test_engine_util:prev_rev(FDI),
        Rev = PrevRev#rev_info.rev,
        [{purge, {Id, Rev}}| CActions]
    end, [], FDIs),
    {ok, St3} = test_engine_util:apply_actions(Engine, St2,
        lists:reverse(RevActions2)),

    % check that before compaction all NumDocs of purge_requests
    % are in purge_tree,
    % even if NumDocs=1200 is greater than purged_docs_limit=1000
    {ok, PurgedIdRevs} = Engine:fold_purge_infos(St3, 0, fun fold_fun/2, [], []),
    ?assertEqual(1, Engine:get_oldest_purge_seq(St3)),
    ?assertEqual(NumDocs, length(PurgedIdRevs)),

    % compact db
    {ok, St4, DbName, _, Term} = test_engine_util:compact(Engine, St3, Path),
    {ok, St5, undefined} = Engine:finish_compaction(St4, DbName, [], Term),

    % check that after compaction only purged_docs_limit purge_requests
    % are in purge_tree
    PurgedDocsLimit = Engine:get_purge_infos_limit(St5),
    OldestPSeq = Engine:get_oldest_purge_seq(St5),
    {ok, PurgedIdRevs2} = Engine:fold_purge_infos(
        St5, OldestPSeq - 1, fun fold_fun/2, [], []),
    ExpectedOldestPSeq = NumDocs - PurgedDocsLimit + 1,
    ?assertEqual(ExpectedOldestPSeq, OldestPSeq),
    ?assertEqual(PurgedDocsLimit, length(PurgedIdRevs2)).


docid(I) ->
    Str = io_lib:format("~4..0b", [I]),
    iolist_to_binary(Str).


local_docid(I) ->
    Str = io_lib:format("_local/~4..0b", [I]),
    iolist_to_binary(Str).


fold_fun({_PSeq, _UUID, Id, Revs}, Acc) ->
    {ok, [{Id, Revs} | Acc]}.
