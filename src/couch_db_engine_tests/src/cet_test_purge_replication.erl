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
    cet_util:setup_mod([mem3, fabric]).


cet_purge_repl_disabled() ->
    cet_util:with_config([{"mem3", "replicate_purges", "false"}], fun() ->
        {ok, SrcDb, TgtDb} = create_db_pair(),
        repl(SrcDb, TgtDb),

        Actions1 = [
            {create, {<<"foo1">>, {[{<<"vsn">>, 1}]}}},
            {create, {<<"foo2">>, {[{<<"vsn">>, 2}]}}}
        ],
        ok = cet_util:apply_actions(SrcDb, Actions1),
        repl(SrcDb, TgtDb),

        Actions2 = [
            {purge, {<<"foo1">>, prev_rev(SrcDb, <<"foo1">>)}}
        ],
        ok = cet_util:apply_actions(SrcDb, Actions2),

        Actions3 = [
            {purge, {<<"foo2">>, prev_rev(TgtDb, <<"foo2">>)}}
        ],
        ok = cet_util:apply_actions(TgtDb, Actions3),

        SrcShard = make_shard(SrcDb),
        TgtShard = make_shard(TgtDb),
        ?assertEqual({ok, 0}, mem3_rep:go(SrcShard, TgtShard)),

        ?assertMatch([#full_doc_info{}], open_docs(SrcDb, [<<"foo2">>])),
        ?assertMatch([#full_doc_info{}], open_docs(TgtDb, [<<"foo1">>]))
    end).


cet_purge_repl_simple_pull() ->
    {ok, SrcDb, TgtDb} = create_db_pair(),
    repl(SrcDb, TgtDb),

    Actions1 = [
        {create, {<<"foo">>, {[{<<"vsn">>, 1}]}}}
    ],
    ok = cet_util:apply_actions(SrcDb, Actions1),
    repl(SrcDb, TgtDb),

    Actions2 = [
        {purge, {<<"foo">>, prev_rev(TgtDb, <<"foo">>)}}
    ],
    ok = cet_util:apply_actions(TgtDb, Actions2),
    repl(SrcDb, TgtDb).


cet_purge_repl_simple_push() ->
    {ok, SrcDb, TgtDb} = create_db_pair(),
    repl(SrcDb, TgtDb),

    Actions1 = [
        {create, {<<"foo">>, {[{<<"vsn">>, 1}]}}}
    ],
    ok = cet_util:apply_actions(SrcDb, Actions1),
    repl(SrcDb, TgtDb),

    Actions2 = [
        {purge, {<<"foo">>, prev_rev(SrcDb, <<"foo">>)}}
    ],
    ok = cet_util:apply_actions(SrcDb, Actions2),
    repl(SrcDb, TgtDb).


create_db_pair() ->
    {ok, SrcDb} = cet_util:create_db(),
    {ok, TgtDb} = cet_util:create_db(),
    try
        {ok, couch_db:name(SrcDb), couch_db:name(TgtDb)}
    after
        couch_db:close(SrcDb),
        couch_db:close(TgtDb)
    end.


repl(SrcDb, TgtDb) ->
    SrcShard = make_shard(SrcDb),
    TgtShard = make_shard(TgtDb),

    ?assertEqual({ok, 0}, mem3_rep:go(SrcShard, TgtShard)),

    SrcTerm = cet_util:db_as_term(SrcDb, replication),
    TgtTerm = cet_util:db_as_term(TgtDb, replication),
    Diff = cet_util:term_diff(SrcTerm, TgtTerm),
    ?assertEqual(nodiff, Diff).


make_shard(DbName) ->
    #shard{
        name = DbName,
        node = node(),
        dbname = DbName,
        range = [0, 16#FFFFFFFF]
    }.


open_docs(DbName, DocIds) ->
    {ok, Db} = couch_db:open_int(DbName, [?ADMIN_CTX]),
    try
        couch_db_engine:open_docs(Db, DocIds)
    after
        couch_db:close(Db)
    end.


prev_rev(DbName, DocId) ->
    [FDI] = open_docs(DbName, [DocId]),
    PrevRev = cet_util:prev_rev(FDI),
    PrevRev#rev_info.rev.
