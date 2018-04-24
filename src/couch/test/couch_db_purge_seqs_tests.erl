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

-module(couch_db_purge_seqs_tests).

-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch/include/couch_db.hrl").


setup() ->
    DbName = ?tempdb(),
    {ok, _Db} = create_db(DbName),
    DbName.

teardown(DbName) ->
    delete_db(DbName),
    ok.

couch_db_purge_seqs_test_() ->
    {
        "Couch_db purge_seqs",
        [
            {
                setup,
                fun test_util:start_couch/0, fun test_util:stop_couch/1,
                [couch_db_purge_seqs()]
            }
        ]
    }.


couch_db_purge_seqs() ->
    {
       foreach,
            fun setup/0, fun teardown/1,
            [
                fun test_update_seq_bounce/1,
                fun test_update_seq_inc_on_complete_purge/1,
                fun test_purge_seq_bounce/1,
                fun test_fold_purge_infos/1,
                fun test_purge_seq/1
        ]
    }.

test_update_seq_bounce(DbName) ->
    ?_test(
        begin
            {ok, Db} = couch_db:open_int(DbName, []),
            Doc1 = {[{<<"_id">>, <<"foo1">>}, {<<"vsn">>, 1.1}]},
            Doc2 = {[{<<"_id">>, <<"foo2">>}, {<<"vsn">>, 1.2}]},
            {ok, Rev} = save_doc(Db, Doc1),
            {ok, _Rev2} = save_doc(Db, Doc2),
            couch_db:ensure_full_commit(Db),

            {ok, Db2} = couch_db:reopen(Db),
            ?assertEqual(2, couch_db_engine:get_doc_count(Db2)),
            ?assertEqual(2, couch_db_engine:get_update_seq(Db2)),

            UUID = couch_uuids:new(),
            {ok, [{ok, PRevs}]} = couch_db:purge_docs(
                Db2, [{UUID, <<"foo1">>, [Rev]}]
            ),

            ?assertEqual([Rev], PRevs),

            {ok, Db3} = couch_db:reopen(Db2),
            {ok, _PIdsRevs} = couch_db:fold_purge_infos(
                Db3, 0, fun fold_fun/2, [], []),
            ?assertEqual(3, couch_db_engine:get_update_seq(Db3))
        end).


test_update_seq_inc_on_complete_purge(DbName) ->
    ?_test(
        begin
            {ok, Db} = couch_db:open_int(DbName, []),
            Doc1 = {[{<<"_id">>, <<"foo1">>}, {<<"vsn">>, 1.1}]},
            Doc2 = {[{<<"_id">>, <<"foo2">>}, {<<"vsn">>, 1.2}]},
            {ok, Rev} = save_doc(Db, Doc1),
            {ok, _Rev2} = save_doc(Db, Doc2),
            couch_db:ensure_full_commit(Db),

            {ok, Db2} = couch_db:reopen(Db),
            ?assertEqual(2, couch_db_engine:get_doc_count(Db2)),
            ?assertEqual(2, couch_db_engine:get_update_seq(Db2)),

            UUID = couch_uuids:new(),
            {ok, [{ok, PRevs}]} = couch_db:purge_docs(
                Db2, [{UUID, <<"invalid">>, [Rev]}]
            ),

            {ok, Db3} = couch_db:reopen(Db2),
            ?assertEqual(2, couch_db_engine:get_update_seq(Db3)),

            ?assertEqual([], PRevs),

            UUID2 = couch_uuids:new(),
            {ok, [{ok, PRevs2}]} = couch_db:purge_docs(
                Db3, [{UUID2, <<"foo1">>, [Rev]}]
            ),

            ?assertEqual([Rev], PRevs2),

            {ok, Db4} = couch_db:reopen(Db3),
            ?assertEqual(3, couch_db_engine:get_update_seq(Db4))
        end).


test_purge_seq_bounce(DbName) ->
    ?_test(
        begin
            {ok, Db} = couch_db:open_int(DbName, []),
            Doc1 = {[{<<"_id">>, <<"foo1">>}, {<<"vsn">>, 1.1}]},
            Doc2 = {[{<<"_id">>, <<"foo2">>}, {<<"vsn">>, 1.2}]},
            {ok, Rev} = save_doc(Db, Doc1),
            {ok, _Rev2} = save_doc(Db, Doc2),
            couch_db:ensure_full_commit(Db),

            {ok, Db2} = couch_db:reopen(Db),
            ?assertEqual(2, couch_db_engine:get_doc_count(Db2)),
            ?assertEqual(0, couch_db_engine:get_purge_seq(Db2)),

            UUID = couch_uuids:new(),
            {ok, [{ok, PRevs}]} = couch_db:purge_docs(
                Db2, [{UUID, <<"foo1">>, [Rev]}]
            ),

            ?assertEqual([Rev], PRevs),

            {ok, Db3} = couch_db:reopen(Db2),
            {ok, _PIdsRevs} = couch_db:fold_purge_infos(
                Db3, 0, fun fold_fun/2, [], []),
            ?assertEqual(1, couch_db_engine:get_purge_seq(Db3))
        end).


test_fold_purge_infos(DbName) ->
    ?_test(
        begin
            {ok, Db} = couch_db:open_int(DbName, []),
            Doc1 = {[{<<"_id">>, <<"foo1">>}, {<<"vsn">>, 1.1}]},
            Doc2 = {[{<<"_id">>, <<"foo2">>}, {<<"vsn">>, 1.2}]},
            {ok, Rev} = save_doc(Db, Doc1),
            {ok, Rev2} = save_doc(Db, Doc2),
            couch_db:ensure_full_commit(Db),

            {ok, Db2} = couch_db:reopen(Db),
            ?assertEqual(2, couch_db_engine:get_doc_count(Db2)),
            ?assertEqual(0, couch_db_engine:get_del_doc_count(Db2)),
            ?assertEqual(2, couch_db_engine:get_update_seq(Db2)),
            ?assertEqual(0, couch_db_engine:get_purge_seq(Db2)),

            UUID = couch_uuids:new(), UUID2 = couch_uuids:new(),
            {ok, [{ok, PRevs}, {ok, PRevs2}]} = couch_db:purge_docs(
                Db2, [{UUID, <<"foo1">>, [Rev]}, {UUID2, <<"foo2">>, [Rev2]}]
            ),

            ?assertEqual([Rev], PRevs),
            ?assertEqual([Rev2], PRevs2),

            {ok, Db3} = couch_db:reopen(Db2),
            {ok, PIdsRevs} = couch_db:fold_purge_infos(
                Db3, 0, fun fold_fun/2, [], []),
            ?assertEqual(0, couch_db_engine:get_doc_count(Db3)),
            ?assertEqual(0, couch_db_engine:get_del_doc_count(Db3)),
            ?assertEqual(3, couch_db_engine:get_update_seq(Db3)),
            ?assertEqual(2, couch_db_engine:get_purge_seq(Db3)),
            ?assertEqual([{<<"foo2">>, [Rev2]}, {<<"foo1">>, [Rev]}], PIdsRevs)
        end).


test_purge_seq(DbName) ->
    ?_test(
        begin
            {ok, Db} = couch_db:open_int(DbName, []),
            Doc1 = {[{<<"_id">>, <<"foo1">>}, {<<"vsn">>, 1.1}]},
            Doc2 = {[{<<"_id">>, <<"foo2">>}, {<<"vsn">>, 1.2}]},
            {ok, Rev} = save_doc(Db, Doc1),
            {ok, _Rev2} = save_doc(Db, Doc2),
            couch_db:ensure_full_commit(Db),

            {ok, Db2} = couch_db:reopen(Db),
            ?assertEqual(2, couch_db_engine:get_doc_count(Db2)),
            UUID = couch_uuids:new(),
            {ok, [{ok, PRevs}]} = couch_db:purge_docs(
                Db2, [{UUID, <<"foo1">>, [Rev]}]
            ),

            ?assertEqual([Rev], PRevs),
            ?assertEqual(0, couch_db_engine:get_purge_seq(Db2)),

            {ok, Db3} = couch_db:reopen(Db2),
            ?assertEqual(1, couch_db_engine:get_purge_seq(Db3))
        end).


create_db(DbName) ->
    couch_db:create(DbName, [?ADMIN_CTX, overwrite]).

delete_db(DbName) ->
    couch_server:delete(DbName, [?ADMIN_CTX]).

save_doc(Db, Json) ->
    Doc = couch_doc:from_json_obj(Json),
    couch_db:update_doc(Db, Doc, []).

fold_fun({_PSeq, _UUID, Id, Revs}, Acc) ->
    {ok, [{Id, Revs} | Acc]}.
