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

-module(couch_db_purge_checkpoint_tests).

-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch/include/couch_db.hrl").


setup() ->
    DbName = ?tempdb(),
    {ok, _Db} = create_db(DbName),
    DbName.

teardown(DbName) ->
    delete_db(DbName),
    ok.

couch_db_purge_checkpoint_test_() ->
    {
        "Couch_db purge_checkpoint",
        [
            {
                setup,
                fun test_util:start_couch/0, fun test_util:stop_couch/1,
                [couch_db_purge_checkpoint()]
            }
        ]

    }.


couch_db_purge_checkpoint() ->
    {
       foreach,
            fun setup/0, fun teardown/1,
            [
                fun test_purge_cp_bad_purgeseq/1,
                fun test_purge_cp_bad_verify_mod/1,
                fun test_purge_cp_bad_verify_fun/1,
                fun test_purge_cp_verify_fun_with_throw/1,
                fun test_purge_cp_verify_fun_without_boolean_rc/1
            ]
    }.


test_purge_cp_bad_purgeseq(DbName) ->
    ?_test(
        begin
            {ok, Db} = couch_db:open_int(DbName, []),
            update_local_purge_doc(
                Db,
                "<<bad_purgeseq>>",
                bad_purgeseq,
                <<"couch_db_purge_checkpoint_tests">>,
                <<"normal_verify_fun">>
            ),
            {ok, Db2} = couch_db:reopen(Db),
            Result = try
                couch_db:get_minimum_purge_seq(Db2)
            catch _:_ ->
                failed
            end,
            ?assertEqual(0, Result)
        end).


test_purge_cp_bad_verify_mod(DbName) ->
    ?_test(
        begin
            {ok, Db} = couch_db:open_int(DbName, []),
            update_local_purge_doc(
                Db,
                "<<bad_verify_module>>",
                1,
                [invalid_module],
                <<"valid_verify_fun">>

            ),
            {ok, Db2} = couch_db:reopen(Db),
            FoldFun = fun(#doc{id = DocId, body = {Props}}, SeqAcc) ->
                case DocId of
                    <<"_local/purge-", _/binary>> ->
                        try
                            couch_db:get_purge_client_fun(DocId, Props)
                        catch failed ->
                            {ok, badarg}
                        end;
                    _ ->
                        {stop, SeqAcc}
                end
            end,
            Opts = [
                {start_key, list_to_binary(?LOCAL_DOC_PREFIX ++ "purge-")}
            ],
            {ok, Result} = couch_db:fold_local_docs(Db2, FoldFun, 0, Opts),
            ?assertEqual(badarg, Result)
        end).


test_purge_cp_bad_verify_fun(DbName) ->
    ?_test(
        begin
            {ok, Db} = couch_db:open_int(DbName, []),
            update_local_purge_doc(
                Db,
                "<<bad_verify_fun>>",
                1,
                <<"couch_db_purge_checkpoint_tests">>,
                [invalid_function]
            ),
            {ok, Db2} = couch_db:reopen(Db),
            FoldFun = fun(#doc{id = DocId, body = {Props}}, SeqAcc) ->
                case DocId of
                    <<"_local/purge-", _/binary>> ->
                        try
                            couch_db:get_purge_client_fun(DocId, Props)
                        catch failed ->
                            {ok, badarg}
                        end;
                    _ ->
                        {stop, SeqAcc}
                end
            end,
            Opts = [
                {start_key, list_to_binary(?LOCAL_DOC_PREFIX ++ "purge-")}
            ],
            {ok, Result} = couch_db:fold_local_docs(Db2, FoldFun, 0, Opts),
            ?assertEqual(badarg, Result)
        end).


test_purge_cp_verify_fun_with_throw(DbName) ->
    ?_test(
        begin
            {ok, Db} = couch_db:open_int(DbName, []),
            update_local_purge_doc(
                Db,
                "<<bad_verify_fun_with_throw>>",
                1,
                <<"couch_db_purge_checkpoint_tests">>,
                <<"verify_fun_with_throw">>
            ),
            {ok, Db2} = couch_db:reopen(Db),
            FoldFun = fun(#doc{id = DocId, body = {Props}}, SeqAcc) ->
                case DocId of
                    <<"_local/purge-", _/binary>> ->
                        case couch_db:purge_client_exists(Db, DocId, Props) of
                            true -> {ok, {true, SeqAcc}};
                            false -> {ok, {false, SeqAcc}}
                        end;
                    _ ->
                        {stop, SeqAcc}
                end
                      end,
            Opts = [
                {start_key, list_to_binary(?LOCAL_DOC_PREFIX ++ "purge-")}
            ],
            {ok, Result} = couch_db:fold_local_docs(Db2, FoldFun, 0, Opts),
            ?assertEqual({true,0}, Result)
        end).


test_purge_cp_verify_fun_without_boolean_rc(DbName) ->
    ?_test(
        begin
            {ok, Db} = couch_db:open_int(DbName, []),
            update_local_purge_doc(
                Db,
                "<<verify_fun_without_boolean_rc>>",
                1,
                <<"couch_db_purge_checkpoint_tests">>,
                <<"verify_fun_without_bool_rc">>
            ),
            {ok, Db2} = couch_db:reopen(Db),
            FoldFun = fun(#doc{id = DocId, body = {Props}}, SeqAcc) ->
                case DocId of
                    <<"_local/purge-", _/binary>> ->
                        case couch_db:purge_client_exists(Db, DocId, Props) of
                            true -> {ok, {true, SeqAcc}};
                            false -> {ok, {false, SeqAcc}}
                        end;
                    _ ->
                        {stop, SeqAcc}
                end
                      end,
            Opts = [
                {start_key, list_to_binary(?LOCAL_DOC_PREFIX ++ "purge-")}
            ],
            {ok, Result} = couch_db:fold_local_docs(Db2, FoldFun, 0, Opts),
            ?assertEqual({true,0}, Result)
        end).


create_db(DbName) ->
    couch_db:create(DbName, [?ADMIN_CTX, overwrite]).

delete_db(DbName) ->
    couch_server:delete(DbName, [?ADMIN_CTX]).

get_local_purge_doc_id(Sig) ->
    Version = "v" ++ config:get("purge", "version", "1") ++ "-",
    ?l2b(?LOCAL_DOC_PREFIX ++ "purge-" ++ Version ++ "test-" ++ Sig).

update_local_purge_doc(Db, Sig, PurgeSeq, Mod, Fun) ->
    {Mega, Secs, _} = os:timestamp(),
    NowSecs = Mega * 1000000 + Secs,
    Doc = couch_doc:from_json_obj({[
        {<<"_id">>, get_local_purge_doc_id(Sig)},
        {<<"purge_seq">>, PurgeSeq},
        {<<"timestamp_utc">>, NowSecs},
        {<<"verify_module">>, Mod},
        {<<"verify_function">>, Fun},
        {<<"verify_options">>, {[
            {<<"signature">>, Sig}
        ]}},
        {<<"type">>, <<"test">>}
    ]}),
    couch_db:update_doc(Db, Doc, []).

valid_verify_fun(_Options) ->
    true.

verify_fun_with_throw(_Options) ->
    throw(failed).

verify_fun_without_bool_rc(_Options) ->
    ok.
