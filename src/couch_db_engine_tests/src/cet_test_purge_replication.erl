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

-module(cet_test_purge_replication).
-compile(export_all).
-compile(nowarn_export_all).


-include_lib("eunit/include/eunit.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("mem3/include/mem3.hrl").


setup_mod() ->
    cet_util:setup_mod([mem3, fabric, couch_replicator]).


setup_test() ->
    {ok, Src} = cet_util:create_db(),
    {ok, Tgt} = cet_util:create_db(),
    {couch_db:name(Src), couch_db:name(Tgt)}.


teardown_test({SrcDb, TgtDb}) ->
    ok = couch_server:delete(SrcDb, []),
    ok = couch_server:delete(TgtDb, []).


cet_purge_http_replication({Source, Target}) ->
    {ok, Rev1} = cet_util:save_doc(Source, {[{'_id', foo}, {vsn, 1}]}),

    cet_util:assert_db_props(Source, [
        {doc_count, 1},
        {del_doc_count, 0},
        {update_seq, 1},
        {changes, 1},
        {purge_seq, 0},
        {purge_infos, []}
    ]),

    RepObject = {[
        {<<"source">>, Source},
        {<<"target">>, Target}
    ]},

    {ok, _} = couch_replicator:replicate(RepObject, ?ADMIN_USER),
    {ok, Doc1} = cet_util:open_doc(Target, foo),

    cet_util:assert_db_props(Target, [
        {doc_count, 1},
        {del_doc_count, 0},
        {update_seq, 1},
        {changes, 1},
        {purge_seq, 0},
        {purge_infos, []}
    ]),

    PurgeInfos = [
        {cet_util:uuid(), <<"foo">>, [Rev1]}
    ],

    {ok, [{ok, PRevs}]} = cet_util:purge(Source, PurgeInfos),
    ?assertEqual([Rev1], PRevs),

    cet_util:assert_db_props(Source, [
        {doc_count, 0},
        {del_doc_count, 0},
        {update_seq, 2},
        {changes, 0},
        {purge_seq, 1},
        {purge_infos, PurgeInfos}
    ]),

    % Show that a purge on the source is
    % not replicated to the target
    {ok, _} = couch_replicator:replicate(RepObject, ?ADMIN_USER),
    {ok, Doc2} = cet_util:open_doc(Target, foo),
    [Rev2] = Doc2#doc_info.revs,
    ?assertEqual(Rev1, Rev2#rev_info.rev),
    ?assertEqual(Doc1, Doc2),

    cet_util:assert_db_props(Target, [
        {doc_count, 1},
        {del_doc_count, 0},
        {update_seq, 1},
        {changes, 1},
        {purge_seq, 0},
        {purge_infos, []}
    ]),

    % Show that replicating from the target
    % back to the source reintroduces the doc
    RepObject2 = {[
        {<<"source">>, Target},
        {<<"target">>, Source}
    ]},

    {ok, _} = couch_replicator:replicate(RepObject2, ?ADMIN_USER),
    {ok, Doc3} = cet_util:open_doc(Source, foo),
    [Revs3] = Doc3#doc_info.revs,
    ?assertEqual(Rev1, Revs3#rev_info.rev),

    cet_util:assert_db_props(Source, [
        {doc_count, 1},
        {del_doc_count, 0},
        {update_seq, 3},
        {changes, 1},
        {purge_seq, 1},
        {purge_infos, PurgeInfos}
    ]).


cet_purge_internal_repl_disabled({Source, Target}) ->
    cet_util:with_config([{"mem3", "replicate_purges", "false"}], fun() ->
        repl(Source, Target),

        {ok, [Rev1, Rev2]} = cet_util:save_docs(Source, [
            {[{'_id', foo1}, {vsn, 1}]},
            {[{'_id', foo2}, {vsn, 2}]}
        ]),

        repl(Source, Target),

        PurgeInfos1 = [
            {cet_util:uuid(), <<"foo1">>, [Rev1]}
        ],
        {ok, [{ok, PRevs1}]} = cet_util:purge(Source, PurgeInfos1),
        ?assertEqual([Rev1], PRevs1),

        PurgeInfos2 = [
            {cet_util:uuid(), <<"foo2">>, [Rev2]}
        ],
        {ok, [{ok, PRevs2}]} = cet_util:purge(Target, PurgeInfos2),
        ?assertEqual([Rev2], PRevs2),

        SrcShard = make_shard(Source),
        TgtShard = make_shard(Target),
        ?assertEqual({ok, 0}, mem3_rep:go(SrcShard, TgtShard)),
        ?assertEqual({ok, 0}, mem3_rep:go(TgtShard, SrcShard)),

        ?assertMatch({ok, #doc_info{}}, cet_util:open_doc(Source, <<"foo2">>)),
        ?assertMatch({ok, #doc_info{}}, cet_util:open_doc(Target, <<"foo1">>))
    end).


cet_purge_repl_simple_pull({Source, Target}) ->
    repl(Source, Target),

    {ok, Rev} = cet_util:save_doc(Source, {[{'_id', foo}, {vsn, 1}]}),
    repl(Source, Target),

    PurgeInfos = [
        {cet_util:uuid(), <<"foo">>, [Rev]}
    ],
    {ok, [{ok, PRevs}]} = cet_util:purge(Target, PurgeInfos),
    ?assertEqual([Rev], PRevs),
    repl(Source, Target).


cet_purge_repl_simple_push({Source, Target}) ->
    repl(Source, Target),

    {ok, Rev} = cet_util:save_doc(Source, {[{'_id', foo}, {vsn, 1}]}),
    repl(Source, Target),

    PurgeInfos = [
        {cet_util:uuid(), <<"foo">>, [Rev]}
    ],
    {ok, [{ok, PRevs}]} = cet_util:purge(Source, PurgeInfos),
    ?assertEqual([Rev], PRevs),
    repl(Source, Target).


repl(Source, Target) ->
    SrcShard = make_shard(Source),
    TgtShard = make_shard(Target),

    ?assertEqual({ok, 0}, mem3_rep:go(SrcShard, TgtShard)),

    SrcTerm = cet_util:db_as_term(Source, replication),
    TgtTerm = cet_util:db_as_term(Target, replication),
    Diff = cet_util:term_diff(SrcTerm, TgtTerm),
    ?assertEqual(nodiff, Diff).


make_shard(DbName) ->
    #shard{
        name = DbName,
        node = node(),
        dbname = DbName,
        range = [0, 16#FFFFFFFF]
    }.
