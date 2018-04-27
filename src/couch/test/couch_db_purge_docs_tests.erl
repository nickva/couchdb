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

-module(couch_db_purge_docs_tests).

-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch/include/couch_db.hrl").


-define(REV_DEPTH, 100).


couch_db_purge_docs_test_() ->
    {
        "Couch_db purge_docs",
        [
            {
                setup,
                fun test_util:start_couch/0,
                fun test_util:stop_couch/1,
                [
                    couch_db_purge_docs()
                ]
            },
            {
                setup,
                fun start_with_replicator/0,
                fun test_util:stop_couch/1,
                [
                    couch_db_purge_with_replication()
                ]
            }
        ]
    }.


couch_db_purge_docs() ->
    {
        foreach,
        fun setup/0,
        fun teardown/1,
        [
            fun test_purge_2_to_purge_3/1,
            fun test_purge_all/1,
            fun test_purge_some/1,
            fun test_purge_none/1,
            fun test_purge_missing_docid/1,
            fun test_purge_repeated_docid/1,
            fun test_purge_repeated_rev/1,
            fun test_purge_partial/1,
            fun test_all_removal_purges/1,
            fun test_purge_invalid_rev/1,
            fun test_purge_duplicate_UUID/1,
            fun test_purge_id_not_exist/1,
            fun test_purge_non_leaf_rev/1,
            fun test_purge_deep_tree/1
        ]
    }.


couch_db_purge_with_replication() ->
    {
        foreach,
        fun setup_replicator/0,
        fun teardown_replicator/1,
        [
            fun test_purge_with_replication/1
        ]
    }.


start_with_replicator() ->
    test_util:start_couch([couch_replicator]).


setup() ->
    DbName = ?tempdb(),
    {ok, _Db} = create_db(DbName),
    DbName.


teardown(DbName) ->
    delete_db(DbName),
    ok.


setup_replicator() ->
    Source = ?tempdb(),
    Target = ?tempdb(),
    {ok, _} = create_db(Source),
    {ok, _} = create_db(Target),
    {Source, Target}.


teardown_replicator({Source, Target}) ->
    delete_db(Source),
    delete_db(Target),
    ok.


test_purge_2_to_purge_3(DbName) ->
    ?_test(begin
        {ok, Rev} = save_doc(DbName, {[{'_id', foo1}, {vsn, 1.1}]}),

        PurgeInfos = [
            {uuid(), <<"foo1">>, [Rev]}
        ],

        {ok, [{ok, PRevs}]} = purge(DbName, PurgeInfos),
        ?assertEqual([Rev], PRevs),

        assertProps(DbName, [
            {doc_count, 0},
            {del_doc_count, 0},
            {update_seq, 2},
            {changes, 0},
            {purge_seq, 1},
            {purge_infos, PurgeInfos}
        ])
    end).


test_purge_all(DbName) ->
    ?_test(begin
        {ok, [Rev1, Rev2]} = save_docs(DbName, [
            {[{'_id', foo1}, {vsn, 1.1}]},
            {[{'_id', foo2}, {vsn, 1.2}]}
        ]),

        assertProps(DbName, [
            {doc_count, 2},
            {del_doc_count, 0},
            {update_seq, 2},
            {changes, 2},
            {purge_seq, 0},
            {purge_infos, []}
        ]),

        PurgeInfos = [
            {uuid(), <<"foo1">>, [Rev1]},
            {uuid(), <<"foo2">>, [Rev2]}
        ],

        {ok, [{ok, PRevs1}, {ok, PRevs2}]} = purge(DbName, PurgeInfos),

        ?assertEqual([Rev1], PRevs1),
        ?assertEqual([Rev2], PRevs2),

        assertProps(DbName, [
            {doc_count, 0},
            {del_doc_count, 0},
            {update_seq, 3},
            {changes, 0},
            {purge_seq, 2},
            {purge_infos, PurgeInfos}
        ])
    end).


test_all_removal_purges(DbName) ->
    ?_test(begin
        {ok, Rev1} = save_doc(DbName, {[{'_id', foo}, {vsn, 1}]}),
        Update = {[
            {<<"_id">>, <<"foo">>},
            {<<"_rev">>, couch_doc:rev_to_str(Rev1)},
            {<<"_deleted">>, true},
            {<<"vsn">>, 2}
        ]},
        {ok, Rev2} = save_doc(DbName, Update),

        assertProps(DbName, [
            {doc_count, 0},
            {del_doc_count, 1},
            {update_seq, 2},
            {changes, 1},
            {purge_seq, 0},
            {purge_infos, []}
        ]),

        PurgeInfos = [
            {uuid(), <<"foo">>, [Rev2]}
        ],

        {ok, [{ok, PRevs}]} = purge(DbName, PurgeInfos),

        ?assertEqual([Rev2], PRevs),

        assertProps(DbName, [
            {doc_count, 0},
            {del_doc_count, 0},
            {update_seq, 3},
            {changes, 0},
            {purge_seq, 1},
            {purge_infos, PurgeInfos}
        ])
    end).


test_purge_some(DbName) ->
    ?_test(begin
        {ok, [Rev1, _Rev2]} = save_docs(DbName, [
            {[{'_id', foo1}, {vsn, 1}]},
            {[{'_id', foo2}, {vsn, 2}]}
        ]),

        assertProps(DbName, [
            {doc_count, 2},
            {del_doc_count, 0},
            {update_seq, 2},
            {changes, 2},
            {purge_seq, 0},
            {purge_infos, []}
        ]),

        PurgeInfos = [
            {uuid(), <<"foo1">>, [Rev1]}
        ],
        {ok, [{ok, PRevs}]} = purge(DbName, PurgeInfos),

        ?assertEqual([Rev1], PRevs),

        assertProps(DbName, [
            {doc_count, 1},
            {del_doc_count, 0},
            {update_seq, 3},
            {changes, 1},
            {purge_seq, 1},
            {purge_infos, PurgeInfos}
        ])
    end).


test_purge_none(DbName) ->
    ?_test(begin
        {ok, [_Rev1, _Rev2]} = save_docs(DbName, [
            {[{'_id', foo1}, {vsn, 1}]},
            {[{'_id', foo2}, {vsn, 2}]}
        ]),

        assertProps(DbName, [
            {doc_count, 2},
            {del_doc_count, 0},
            {update_seq, 2},
            {changes, 2},
            {purge_seq, 0},
            {purge_infos, []}
        ]),

        {ok, []} = purge(DbName, []),

        assertProps(DbName, [
            {doc_count, 2},
            {del_doc_count, 0},
            {update_seq, 2},
            {changes, 2},
            {purge_seq, 0},
            {purge_infos, []}
        ])
    end).


test_purge_missing_docid(DbName) ->
    ?_test(begin
        {ok, [Rev1, _Rev2]} = save_docs(DbName, [
            {[{'_id', foo1}, {vsn, 1}]},
            {[{'_id', foo2}, {vsn, 2}]}
        ]),

        assertProps(DbName, [
            {doc_count, 2},
            {del_doc_count, 0},
            {update_seq, 2},
            {changes, 2},
            {purge_seq, 0},
            {purge_infos, []}
        ]),

        PurgeInfos = [
            {uuid(), <<"baz">>, [Rev1]}
        ],

        {ok, [{ok, []}]} = purge(DbName, PurgeInfos),

        assertProps(DbName, [
            {doc_count, 2},
            {del_doc_count, 0},
            {update_seq, 3},
            {changes, 2},
            {purge_seq, 1},
            {purge_infos, PurgeInfos}
        ])
    end).


test_purge_repeated_docid(DbName) ->
    ?_test(begin
        {ok, [Rev1, _Rev2]} = save_docs(DbName, [
            {[{'_id', foo1}, {vsn, 1}]},
            {[{'_id', foo2}, {vsn, 2}]}
        ]),

        assertProps(DbName, [
            {doc_count, 2},
            {del_doc_count, 0},
            {update_seq, 2},
            {purge_seq, 0},
            {changes, 2},
            {purge_infos, []}
        ]),

        PurgeInfos = [
            {uuid(), <<"foo1">>, [Rev1]},
            {uuid(), <<"foo1">>, [Rev1]}
        ],

        {ok, Resp} = purge(DbName, PurgeInfos),
        ?assertEqual([{ok, [Rev1]}, {ok, []}], Resp),

        assertProps(DbName, [
            {doc_count, 1},
            {del_doc_count, 0},
            {update_seq, 3},
            {purge_seq, 2},
            {changes, 1},
            {purge_infos, PurgeInfos}
        ])
    end).


test_purge_id_not_exist(DbName) ->
    ?_test(begin
        PurgeInfos = [
            {uuid(), <<"foo">>, [{0, <<0>>}]}
        ],

        {ok, [{ok, PRevs}]} = purge(DbName, PurgeInfos),
        ?assertEqual([], PRevs),

        assertProps(DbName, [
            {doc_count, 0},
            {del_doc_count, 0},
            {update_seq, 1},
            {changes, 0},
            {purge_seq, 1},
            {purge_infos, PurgeInfos}
        ])
    end).


test_purge_non_leaf_rev(DbName) ->
    ?_test(begin
        {ok, Rev1} = save_doc(DbName, {[{'_id', foo}, {vsn, 1}]}),
        Update = {[
            {'_id', foo},
            {'_rev', couch_doc:rev_to_str(Rev1)},
            {vsn, 2}
        ]},
        {ok, _Rev2} = save_doc(DbName, Update),

        PurgeInfos = [
            {couch_uuids:new(), <<"foo">>, [Rev1]}
        ],

        {ok, [{ok, PRevs}]} = purge(DbName, PurgeInfos),
        ?assertEqual([], PRevs),

        assertProps(DbName, [
            {doc_count, 1},
            {del_doc_count, 0},
            {update_seq, 3},
            {changes, 1},
            {purge_seq, 1},
            {purge_infos, PurgeInfos}
        ])
    end).


test_purge_invalid_rev(DbName) ->
    ?_test(begin
        {ok, [_Rev1, Rev2]} = save_docs(DbName, [
            {[{'_id', foo1}, {vsn, 1}]},
            {[{'_id', foo2}, {vsn, 2}]}
        ]),

        PurgeInfos = [
            {uuid(), <<"foo1">>, [Rev2]}
        ],

        {ok, [{ok, PRevs}]} = purge(DbName, PurgeInfos),
        ?assertEqual([], PRevs),

        assertProps(DbName, [
            {doc_count, 2},
            {del_doc_count, 0},
            {update_seq, 3},
            {changes, 2},
            {purge_seq, 1},
            {purge_infos, PurgeInfos}
        ])
    end).


test_purge_partial(DbName) ->
    ?_test(begin
        {ok, Rev1} = save_doc(DbName, {[{'_id', foo}, {vsn, <<"v1.1">>}]}),
        Update = {[
            {'_id', foo},
            {'_rev', couch_doc:rev_to_str({1, [crypto:hash(md5, <<"v1.2">>)]})},
            {vsn, <<"v1.2">>}
        ]},
        {ok, [_Rev2]} = save_docs(DbName, [Update], [replicated_changes]),

        PurgeInfos = [
            {uuid(), <<"foo">>, [Rev1]}
        ],

        {ok, [{ok, PRevs}]} = purge(DbName, PurgeInfos),
        ?assertEqual([Rev1], PRevs),

        assertProps(DbName, [
            {doc_count, 1},
            {del_doc_count, 0},
            {update_seq, 3},
            {changes, 1},
            {purge_seq, 1},
            {purge_infos, PurgeInfos}
        ])
    end).


test_purge_repeated_rev(DbName) ->
    ?_test(begin
        {ok, Rev1} = save_doc(DbName, {[{'_id', foo}, {vsn, <<"v1.1">>}]}),
        Update = {[
            {'_id', foo},
            {'_rev', couch_doc:rev_to_str({1, [crypto:hash(md5, <<"v1.2">>)]})},
            {vsn, <<"v1.2">>}
        ]},
        {ok, [Rev2]} = save_docs(DbName, [Update], [replicated_changes]),

        assertProps(DbName, [
            {doc_count, 1},
            {del_doc_count, 0},
            {update_seq, 2},
            {changes, 1},
            {purge_seq, 0},
            {purge_infos, []}
        ]),

        PurgeInfos1 = [
            {uuid(), <<"foo">>, [Rev1]},
            {uuid(), <<"foo">>, [Rev1, Rev2]}
        ],

        {ok, [{ok, PRevs1}, {ok, PRevs2}]} = purge(DbName, PurgeInfos1),
        ?assertEqual([Rev1], PRevs1),
        ?assertEqual([Rev2], PRevs2),

        assertProps(DbName, [
            {doc_count, 0},
            {del_doc_count, 0},
            {update_seq, 3},
            {changes, 0},
            {purge_seq, 2},
            {purge_infos, PurgeInfos1}
        ])
    end).


test_purge_deep_tree(DbName) ->
    ?_test(begin
        {ok, InitRev} = save_doc(DbName, {[{'_id', bar}, {vsn, 0}]}),
        LastRev = lists:foldl(fun(Count, PrevRev) ->
            Update = {[
                {'_id', bar},
                {'_rev', couch_doc:rev_to_str(PrevRev)},
                {vsn, Count}
            ]},
            {ok, NewRev} = save_doc(DbName, Update),
            NewRev
        end, InitRev, lists:seq(1, ?REV_DEPTH)),

        PurgeInfos = [
            {uuid(), <<"bar">>, [LastRev]}
        ],

        {ok, [{ok, PRevs}]} = purge(DbName, PurgeInfos),
        ?assertEqual([LastRev], PRevs),

        assertProps(DbName, [
            {doc_count, 0},
            {del_doc_count, 0},
            {update_seq, ?REV_DEPTH + 2},
            {changes, 0},
            {purge_seq, 1},
            {purge_infos, PurgeInfos}
        ])
    end).


test_purge_duplicate_UUID(DbName) ->
    ?_test(begin
        {ok, Rev} = save_doc(DbName, {[{'_id', foo1}, {vsn, 1.1}]}),

        assertProps(DbName, [
            {doc_count, 1},
            {del_doc_count, 0},
            {update_seq, 1},
            {changes, 1},
            {purge_seq, 0},
            {purge_infos, []}
        ]),

        PurgeInfos = [
            {uuid(), <<"foo1">>, [Rev]}
        ],

        {ok, [{ok, PRevs1}]} = purge(DbName, PurgeInfos),
        ?assertEqual([Rev], PRevs1),

        % Attempting to purge a repeated UUID is an error
        ?assertThrow({badreq, _}, purge(DbName, PurgeInfos)),

        % Although we can replicate it in
        {ok, []} = purge(DbName, PurgeInfos, [replicated_changes]),

        assertProps(DbName, [
            {doc_count, 0},
            {del_doc_count, 0},
            {update_seq, 3},
            {changes, 0},
            {purge_seq, 1},
            {purge_infos, PurgeInfos}
        ])
    end).


test_purge_with_replication({Source, Target}) ->
    ?_test(begin
        {ok, Rev1} = save_doc(Source, {[{'_id', foo}, {vsn, 1}]}),

        assertProps(Source, [
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
        {ok, Doc1} = open_doc(Target, foo),

        assertProps(Target, [
            {doc_count, 1},
            {del_doc_count, 0},
            {update_seq, 1},
            {changes, 1},
            {purge_seq, 0},
            {purge_infos, []}
        ]),

        PurgeInfos = [
            {uuid(), <<"foo">>, [Rev1]}
        ],

        {ok, [{ok, PRevs}]} = purge(Source, PurgeInfos),
        ?assertEqual([Rev1], PRevs),

        assertProps(Source, [
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
        {ok, Doc2} = open_doc(Target, foo),
        [Rev2] = Doc2#doc_info.revs,
        ?assertEqual(Rev1, Rev2#rev_info.rev),
        ?assertEqual(Doc1, Doc2),

        assertProps(Target, [
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
        {ok, Doc3} = open_doc(Source, foo),
        [Revs3] = Doc3#doc_info.revs,
        ?assertEqual(Rev1, Revs3#rev_info.rev),

        assertProps(Source, [
            {doc_count, 1},
            {del_doc_count, 0},
            {update_seq, 3},
            {changes, 1},
            {purge_seq, 1},
            {purge_infos, PurgeInfos}
        ])
    end).


create_db(DbName) ->
    couch_db:create(DbName, [?ADMIN_CTX, overwrite]).


delete_db(DbName) ->
    couch_server:delete(DbName, [?ADMIN_CTX]).


save_doc(DbName, Json) ->
    {ok, [Rev]} = save_docs(DbName, [Json], []),
    {ok, Rev}.


save_docs(DbName, JsonDocs) ->
    save_docs(DbName, JsonDocs, []).


save_docs(DbName, JsonDocs, Options) ->
    Docs = lists:map(fun(JDoc) ->
        couch_doc:from_json_obj(?JSON_DECODE(?JSON_ENCODE(JDoc)))
    end, JsonDocs),
    Opts = [full_commit | Options],
    {ok, Db} = couch_db:open_int(DbName, []),
    try
        case lists:member(replicated_changes, Options) of
            true ->
                {ok, []} = couch_db:update_docs(
                        Db, Docs, Opts, replicated_changes),
                {ok, lists:map(fun(Doc) ->
                    {Pos, [RevId | _]} = Doc#doc.revs,
                    {Pos, RevId}
                end, Docs)};
            false ->
                {ok, Resp} = couch_db:update_docs(Db, Docs, Opts),
                {ok, [Rev || {ok, Rev} <- Resp]}
        end
    after
        couch_db:close(Db)
    end.


open_doc(DbName, DocId0) ->
    DocId = ?JSON_DECODE(?JSON_ENCODE(DocId0)),
    {ok, Db} = couch_db:open_int(DbName, []),
    try
        couch_db:get_doc_info(Db, DocId)
    after
        couch_db:close(Db)
    end.


purge(DbName, PurgeInfos) ->
    purge(DbName, PurgeInfos, []).


purge(DbName, PurgeInfos0, Options) when is_list(PurgeInfos0) ->
    PurgeInfos = lists:map(fun({UUID, DocIdJson, Revs}) ->
        {UUID, ?JSON_DECODE(?JSON_ENCODE(DocIdJson)), Revs}
    end, PurgeInfos0),
    {ok, Db} = couch_db:open_int(DbName, []),
    try
        couch_db:purge_docs(Db, PurgeInfos, Options)
    after
        couch_db:close(Db)
    end.


assertProps(DbName, Props) when is_binary(DbName) ->
    {ok, Db} = couch_db:open_int(DbName, []),
    try
        assertEachProp(Db, Props)
    after
        couch_db:close(Db)
    end.


assertEachProp(_Db, []) ->
    ok;
assertEachProp(Db, [{doc_count, Expect} | Rest]) ->
    {ok, DocCount} = couch_db:get_doc_count(Db),
    ?assertEqual(Expect, DocCount),
    assertEachProp(Db, Rest);
assertEachProp(Db, [{del_doc_count, Expect} | Rest]) ->
    {ok, DelDocCount} = couch_db:get_del_doc_count(Db),
    ?assertEqual(Expect, DelDocCount),
    assertEachProp(Db, Rest);
assertEachProp(Db, [{update_seq, Expect} | Rest]) ->
    UpdateSeq = couch_db:get_update_seq(Db),
    ?assertEqual(Expect, UpdateSeq),
    assertEachProp(Db, Rest);
assertEachProp(Db, [{changes, Expect} | Rest]) ->
    {ok, NumChanges} = couch_db:fold_changes(Db, 0, fun fold_changes/2, 0, []),
    ?assertEqual(Expect, NumChanges),
    assertEachProp(Db, Rest);
assertEachProp(Db, [{purge_seq, Expect} | Rest]) ->
    {ok, PurgeSeq} = couch_db:get_purge_seq(Db),
    ?assertEqual(Expect, PurgeSeq),
    assertEachProp(Db, Rest);
assertEachProp(Db, [{purge_infos, Expect} | Rest]) ->
    {ok, PurgeInfos} = couch_db:fold_purge_infos(Db, 0, fun fold_fun/2, [], []),
    ?assertEqual(Expect, lists:reverse(PurgeInfos)),
    assertEachProp(Db, Rest).


fold_fun({_PSeq, UUID, Id, Revs}, Acc) ->
    {ok, [{UUID, Id, Revs} | Acc]}.


fold_changes(_A, Acc) ->
    %io:format(standard_error, "~nCHANGE: ~p~n~n", [_A]),
    {ok, Acc + 1}.


uuid() -> couch_uuids:random().
