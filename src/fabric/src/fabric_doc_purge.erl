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

-module(fabric_doc_purge).


-export([
    go/3
]).


-include_lib("fabric/include/fabric.hrl").
-include_lib("mem3/include/mem3.hrl").


-record(acc, {
    worker_uuids,
    req_count,
    resps,
    w
}).


go(_, [], _) ->
    {ok, []};
go(DbName, IdsRevs, Options) ->
    % Generate our purge requests of {UUID, DocId, Revs}
    {UUIDs, Reqs, Count} = create_reqs(IdsRevs, [], [], 0),

    % Fire off rexi workers for each shard.
    {Workers, WorkerUUIDs} = dict:fold(fun(Shard, ShardReqs, {Ws, WUUIDs}) ->
        #shard{name = ShardDbName, node = Node} = Shard,
        Args = [ShardDbName, ShardReqs, Options],
        Ref = rexi:cast(Node, {fabric_rpc, purge_docs, Args}),
        Worker = Shard#shard{ref=Ref},
        ShardUUIDs = [UUID || {UUID, _Id, _Revs} <- ShardReqs],
        {[Worker | Ws], [{Worker, ShardUUIDs} | WUUIDs]}
    end, {[], []}, group_reqs_by_shard(DbName, Reqs)),

    RexiMon = fabric_util:create_monitors(Workers),
    Timeout = fabric_util:request_timeout(),
    Acc0 = #acc{
        worker_uuids = WorkerUUIDs,
        req_count = Count,
        resps = dict:from_list([{UUID, []} || UUID <- UUIDs]),
        w = w(DbName, Options)
    },
    Acc2 = try rexi_utils:recv(Workers, #shard.ref,
            fun handle_message/3, Acc0, infinity, Timeout) of
        {ok, Acc1} ->
            Acc1;
        {timeout, Acc1} ->
            #acc{
                worker_uuids = WorkerUUIDs,
                resps = Resps
            } = Acc1,
            DefunctWorkers = [Worker || {Worker, _} <- WorkerUUIDs],
            fabric_util:log_timeout(DefunctWorkers, "purge_docs"),
            NewResps = append_errors(timeout, WorkerUUIDs, Resps),
            Acc1#acc{worker_uuids = [], resps = NewResps};
        Else ->
            Else
    after
        rexi_monitor:stop(RexiMon)
    end,

    {ok, format_resps(UUIDs, Acc2)}.


handle_message({rexi_DOWN, _, {_, Node}, _}, _Worker, Acc) ->
    #acc{
        worker_uuids = WorkerUUIDs,
        resps = Resps
    } = Acc,
    Pred = fun({#shard{node = N}, _}) -> N == Node end,
    {Failed, Rest} = lists:partition(Pred, WorkerUUIDs),
    NewResps = append_errors(internal_server_error, Failed, Resps),
    maybe_stop(Acc#acc{worker_uuids = Rest, resps = NewResps});

handle_message({rexi_EXIT, _}, Worker, Acc) ->
    #acc{
        worker_uuids = WorkerUUIDs,
        resps = Resps
    } = Acc,
    {value, WorkerPair, Rest} = lists:keytake(Worker, 1, WorkerUUIDs),
    NewResps = append_errors(internal_server_error, [WorkerPair], Resps),
    maybe_stop(Acc#acc{worker_uuids = Rest, resps = NewResps});

handle_message({ok, Replies}, Worker, Acc) ->
    #acc{
        worker_uuids = WorkerUUIDs,
        resps = Resps
    } = Acc,
    {value, {_W, UUIDs}, Rest} = lists:keytake(Worker, 1, WorkerUUIDs),
    couch_log:error("XKCD: ~p ~p :: ~p ~p", [length(UUIDs), length(Replies), UUIDs, Replies]),
    NewResps = append_resps(UUIDs, Replies, Resps),
    maybe_stop(Acc#acc{worker_uuids = Rest, resps = NewResps});

handle_message({bad_request, Msg}, _, _) ->
    throw({bad_request, Msg}).


create_reqs([], UUIDs, Reqs, Count) ->
    {lists:reverse(UUIDs), lists:reverse(Reqs), Count};

create_reqs([{Id, Revs} | RestIdsRevs], UUIDs, Reqs, Count) ->
    UUID = couch_uuids:new(),
    NewUUIDs = [UUID | UUIDs],
    NewReqs = [{UUID, Id, Revs} | Reqs],
    create_reqs(RestIdsRevs, NewUUIDs, NewReqs, Count + 1).


group_reqs_by_shard(DbName, Reqs) ->
    lists:foldl(fun({_UUID, Id, _Revs} = Req, D0) ->
        lists:foldl(fun(Shard, D1) ->
            dict:append(Shard, Req, D1)
        end, D0, mem3:shards(DbName, Id))
    end, dict:new(), Reqs).


w(DbName, Options) ->
    try
        list_to_integer(couch_util:get_value(w, Options))
    catch _:_ ->
        mem3:quorum(DbName)
    end.


append_errors(Type, WorkerUUIDs, Resps) ->
    lists:foldl(fun({_Worker, UUIDs}, RespAcc) ->
        Errors = [{error, Type} || _UUID <- UUIDs],
        append_resps(UUIDs, Errors, RespAcc)
    end, Resps, WorkerUUIDs).


append_resps([], [], Resps) ->
    Resps;
append_resps([UUID | RestUUIDs], [Reply | RestReplies], Resps) ->
    NewResps = dict:append(UUID, Reply, Resps),
    append_resps(RestUUIDs, RestReplies, NewResps).


maybe_stop(#acc{worker_uuids = []} = Acc) ->
    {stop, Acc};
maybe_stop(#acc{resps = Resps, w = W} = Acc) ->
    try
        dict:fold(fun(_UUID, UUIDResps, _) ->
            couch_log:error("XKCD: ~p ~p", [UUIDResps, W]),
            case has_quorum(UUIDResps, W) of
                true -> ok;
                false -> throw(keep_going)
            end
        end, nil, Resps),
        {stop, Acc}
    catch throw:keep_going ->
        {ok, Acc}
    end.


format_resps(UUIDs, #acc{} = Acc) ->
    #acc{
        resps = Resps,
        w = W
    } = Acc,
    FoldFun = fun(UUID, Replies, ReplyAcc) ->
        OkReplies = [Reply || {ok, Reply} <- Replies],
        case OkReplies of
            [] ->
                [Error | _] = lists:usort(Replies),
                [{UUID, Error} | ReplyAcc];
            _ ->
                AllRevs = lists:usort(lists:flatten(OkReplies)),
                Health = if length(OkReplies) >= W -> ok; true -> accepted end,
                [{UUID, {Health, AllRevs}} | ReplyAcc]
        end
    end,
    FinalReplies = dict:fold(FoldFun, {ok, []}, Resps),
    couch_util:reorder_results(UUIDs, FinalReplies);

format_resps(_UUIDs, Else) ->
    Else.


has_quorum([], W) when W > 0 ->
    false;
has_quorum(_, W) when W =< 0 ->
    true;
has_quorum([{ok, _} | Rest], W) when W > 0 ->
    has_quorum(Rest, W - 1).


%% % eunits
%% doc_purge_ok_test() ->
%%     meck:new(couch_log),
%%     meck:expect(couch_log, warning, fun(_,_) -> ok end),
%%     meck:expect(couch_log, notice, fun(_,_) -> ok end),
%%
%%     Revs1 = [{1, <<"rev11">>}], UUID1 = <<"3de03c5f4c2cd34cc515a9d1ea000abd">>,
%%     UUIDIdRevs1 = {UUID1, <<"id1">>, Revs1},
%%     Revs2 = [{1, <<"rev12">>}], UUID2 = <<"4de03c5f4c2cd34cc515a9d1ea000abc">>,
%%     UUIDIdRevs2 = {UUID2, <<"id2">>, Revs2},
%%     UUIDsIDdsRevs = [UUIDIdRevs1, UUIDIdRevs2],
%%     Shards =
%%         mem3_util:create_partition_map("foo",3,1,["node1","node2","node3"]),
%%     Counters = dict:to_list(
%%         group_idrevs_by_shard_hack(<<"foo">>, Shards, UUIDsIDdsRevs)),
%%     DocsDict = dict:new(),
%%
%%     % ***test for W = 2
%%     AccW2 = {length(Shards), length(UUIDsIDdsRevs), list_to_integer("2"),
%%         Counters, DocsDict},
%%     {ok, {WaitingCountW2_1,_,_,_,_} = AccW2_1} =
%%         handle_message({ok,[{ok, Revs1}, {ok, Revs2}]}, hd(Shards), AccW2),
%%     ?assertEqual(2, WaitingCountW2_1),
%%     {stop, FinalReplyW2 } =
%%         handle_message({ok, [{ok, Revs1}, {ok, Revs2}]},
%%             lists:nth(2,Shards), AccW2_1),
%%     ?assertEqual(
%%         {ok, [{UUID1, {ok, Revs1}}, {UUID2, {ok, Revs2}}]},
%%         FinalReplyW2
%%     ),
%%
%%     % ***test for W = 3
%%     AccW3 = {length(Shards), length(UUIDsIDdsRevs), list_to_integer("3"),
%%         Counters, DocsDict},
%%     {ok, {WaitingCountW3_1,_,_,_,_} = AccW3_1} =
%%         handle_message({ok, [{ok, Revs1}, {ok, Revs2}]}, hd(Shards), AccW3),
%%     ?assertEqual(2, WaitingCountW3_1),
%%     {ok, {WaitingCountW3_2,_,_,_,_} = AccW3_2} =
%%         handle_message({ok,[{ok, Revs1}, {ok, Revs2}]},
%%             lists:nth(2,Shards), AccW3_1),
%%     ?assertEqual(1, WaitingCountW3_2),
%%     {stop, FinalReplyW3 } =
%%         handle_message({ok, [{ok, Revs1}, {ok, Revs2}]},
%%             lists:nth(3,Shards), AccW3_2),
%%     ?assertEqual(
%%         {ok, [{UUID1, {ok, Revs1}}, {UUID2, {ok, Revs2}}]},
%%         FinalReplyW3
%%     ),
%%
%%     % *** test rexi_exit on 1 node
%%     Acc0 = {length(Shards), length(UUIDsIDdsRevs), list_to_integer("2"),
%%         Counters, DocsDict},
%%     {ok, {WaitingCount1,_,_,_,_} = Acc1} =
%%         handle_message({ok, [{ok, Revs1}, {ok, Revs2}]}, hd(Shards), Acc0),
%%     ?assertEqual(2, WaitingCount1),
%%     {ok, {WaitingCount2,_,_,_,_} = Acc2} =
%%         handle_message({rexi_EXIT, nil}, lists:nth(2,Shards), Acc1),
%%     ?assertEqual(1, WaitingCount2),
%%     {stop, Reply} =
%%         handle_message({ok, [{ok, Revs1}, {ok, Revs2}]},
%%             lists:nth(3,Shards), Acc2),
%%     ?assertEqual(
%%         {ok,[{UUID1, {ok, Revs1}}, {UUID2, {ok, Revs2}}]},
%%         Reply
%%     ),
%%
%%     % *** test {error, purge_during_compaction_exceeded_limit} on all nodes
%%     % *** still should return ok reply for the request
%%     ErrPDCEL = {error, purge_during_compaction_exceeded_limit},
%%     Acc20 = {length(Shards), length(UUIDsIDdsRevs), list_to_integer("3"),
%%         Counters, DocsDict},
%%     {ok, {WaitingCount21,_,_,_,_} = Acc21} =
%%         handle_message({ok, [ErrPDCEL, ErrPDCEL]}, hd(Shards), Acc20),
%%     ?assertEqual(2, WaitingCount21),
%%     {ok, {WaitingCount22,_,_,_,_} = Acc22} =
%%         handle_message({ok, [ErrPDCEL, ErrPDCEL]}, lists:nth(2,Shards), Acc21),
%%     ?assertEqual(1, WaitingCount22),
%%     {stop, Reply2 } =
%%         handle_message({ok, [ErrPDCEL, ErrPDCEL]}, lists:nth(3,Shards), Acc22),
%%     ?assertEqual(
%%         {ok, [{UUID1, ErrPDCEL}, {UUID2, ErrPDCEL}]},
%%         Reply2
%%     ),
%%
%%     % *** test {error, purged_docs_limit_exceeded} on all nodes
%%     % *** still should return ok reply for the request
%%     ErrPDLE = {error, purged_docs_limit_exceeded},
%%     Acc30 = {length(Shards), length(UUIDsIDdsRevs), list_to_integer("3"),
%%         Counters, DocsDict},
%%     {ok, {WaitingCount31,_,_,_,_} = Acc31} =
%%         handle_message({ok, [ErrPDLE, ErrPDLE]}, hd(Shards), Acc30),
%%     ?assertEqual(2, WaitingCount31),
%%     {ok, {WaitingCount32,_,_,_,_} = Acc32} =
%%         handle_message({ok, [ErrPDLE, ErrPDLE]}, lists:nth(2,Shards), Acc31),
%%     ?assertEqual(1, WaitingCount32),
%%     {stop, Reply3 } =
%%         handle_message({ok, [ErrPDLE, ErrPDLE]},lists:nth(3,Shards), Acc32),
%%     ?assertEqual(
%%         {ok, [{UUID1, ErrPDLE}, {UUID2, ErrPDLE}]},
%%         Reply3
%%     ),
%%     meck:unload(couch_log).
%%
%%
%% doc_purge_accepted_test() ->
%%     meck:new(couch_log),
%%     meck:expect(couch_log, warning, fun(_,_) -> ok end),
%%     meck:expect(couch_log, notice, fun(_,_) -> ok end),
%%
%%     Revs1 = [{1, <<"rev11">>}], UUID1 = <<"3de03c5f4c2cd34cc515a9d1ea000abd">>,
%%     UUIDIdRevs1 = {UUID1, <<"id1">>, Revs1},
%%     Revs2 = [{1, <<"rev12">>}], UUID2 = <<"4de03c5f4c2cd34cc515a9d1ea000abc">>,
%%     UUIDIdRevs2 = {UUID2, <<"id2">>, Revs2},
%%     UUIDsIDdsRevs = [UUIDIdRevs1, UUIDIdRevs2],
%%     Shards =
%%         mem3_util:create_partition_map("foo",3,1,["node1","node2","node3"]),
%%     Counters = dict:to_list(
%%         group_idrevs_by_shard_hack(<<"foo">>, Shards, UUIDsIDdsRevs)),
%%     DocsDict = dict:new(),
%%
%%     % *** test rexi_exit on 2 nodes
%%     Acc0 = {length(Shards), length(UUIDsIDdsRevs), list_to_integer("2"),
%%         Counters, DocsDict},
%%     {ok, {WaitingCount1,_,_,_,_} = Acc1} =
%%         handle_message({ok, [{ok, Revs1}, {ok, Revs2}]}, hd(Shards), Acc0),
%%     ?assertEqual(2, WaitingCount1),
%%     {ok, {WaitingCount2,_,_,_,_} = Acc2} =
%%         handle_message({rexi_EXIT, nil}, lists:nth(2, Shards), Acc1),
%%     ?assertEqual(1, WaitingCount2),
%%     {stop, Reply} =
%%         handle_message({rexi_EXIT, nil}, lists:nth(3, Shards), Acc2),
%%     ?assertEqual(
%%         {accepted, [{UUID1, {accepted, Revs1}}, {UUID2, {accepted, Revs2}}]},
%%         Reply
%%     ),
%%     meck:unload(couch_log).
%%
%%
%% doc_purge_error_test() ->
%%     meck:new(couch_log),
%%     meck:expect(couch_log, warning, fun(_,_) -> ok end),
%%     meck:expect(couch_log, notice, fun(_,_) -> ok end),
%%
%%     Revs1 = [{1, <<"rev11">>}], UUID1 = <<"3de03c5f4c2cd34cc515a9d1ea000abd">>,
%%     UUIDIdRevs1 = {UUID1, <<"id1">>, Revs1},
%%     Revs2 = [{1, <<"rev12">>}], UUID2 = <<"4de03c5f4c2cd34cc515a9d1ea000abc">>,
%%     UUIDIdRevs2 = {UUID2, <<"id2">>, Revs2},
%%     UUIDsIDdsRevs = [UUIDIdRevs1, UUIDIdRevs2],
%%     Shards =
%%         mem3_util:create_partition_map("foo",3,1,["node1","node2","node3"]),
%%     Counters = dict:to_list(
%%         group_idrevs_by_shard_hack(<<"foo">>, Shards, UUIDsIDdsRevs)),
%%     DocsDict = dict:new(),
%%
%%     % *** test rexi_exit on all 3 nodes
%%     Acc0 = {length(Shards), length(UUIDsIDdsRevs), list_to_integer("2"),
%%         Counters, DocsDict},
%%     {ok, {WaitingCount1,_,_,_,_} = Acc1} =
%%         handle_message({rexi_EXIT, nil}, hd(Shards), Acc0),
%%     ?assertEqual(2, WaitingCount1),
%%     {ok, {WaitingCount2,_,_,_,_} = Acc2} =
%%         handle_message({rexi_EXIT, nil}, lists:nth(2,Shards), Acc1),
%%     ?assertEqual(1, WaitingCount2),
%%     {stop, Reply} =
%%         handle_message({rexi_EXIT, nil}, lists:nth(3,Shards), Acc2),
%%     ?assertEqual(
%%         {error, [{UUID1, {error, internal_server_error}},
%%             {UUID2, {error, internal_server_error}}]},
%%         Reply
%%     ),
%%
%%     % ***test w quorum > # shards, which should fail immediately
%%     Shards2 = mem3_util:create_partition_map("foo",1,1,["node1"]),
%%     Counters2 = dict:to_list(
%%         group_idrevs_by_shard_hack(<<"foo">>, Shards2, UUIDsIDdsRevs)),
%%     AccW4 = {length(Shards2), length(UUIDsIDdsRevs), list_to_integer("2"),
%%         Counters2, DocsDict},
%%     Bool =
%%         case handle_message({ok, [{ok, Revs1}, {ok, Revs2}]},
%%                 hd(Shards), AccW4) of
%%             {stop, _Reply} ->
%%                 true;
%%             _ -> false
%%         end,
%%     ?assertEqual(true, Bool),
%%
%%     % *** test Docs with no replies should end up as {error, internal_server_error}
%%     SA1 = #shard{node = a, range = [1]},
%%     SA2 = #shard{node = a, range = [2]},
%%     SB1 = #shard{node = b, range = [1]},
%%     SB2 = #shard{node = b, range = [2]},
%%     Counters3 = [{SA1,[UUID1]}, {SB1,[UUID1]},
%%         {SA2,[UUID2]}, {SB2,[UUID2]}],
%%     Acc30 = {length(Counters3), length(UUIDsIDdsRevs), 2, Counters3, DocsDict},
%%     {ok, Acc31} = handle_message({ok, [{ok, Revs1}]}, SA1, Acc30),
%%     {ok, Acc32} = handle_message({rexi_EXIT, nil}, SB1, Acc31),
%%     {ok, Acc33} = handle_message({rexi_EXIT, nil}, SA2, Acc32),
%%     {stop, Acc34} = handle_message({rexi_EXIT, nil}, SB2, Acc33),
%%     ?assertEqual(
%%         {error, [{UUID1, {accepted, Revs1}},
%%             {UUID2, {error, internal_server_error}}]},
%%         Acc34
%%     ),
%%     meck:unload(couch_log).
%%
%%
%% % needed for testing to avoid having to start the mem3 application
%% group_idrevs_by_shard_hack(_DbName, Shards, UUIDsIdsRevs) ->
%%     lists:foldl(fun({UUID, _Id, _Revs}, Dict0) ->
%%         lists:foldl(fun(Shard, Dict1) ->
%%             dict:append(Shard, UUID, Dict1)
%%         end, Dict0, Shards)
%%     end, dict:new(), UUIDsIdsRevs).
