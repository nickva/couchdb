% Licensed under the Apache License, Version 2.0 (the "License");
% you may not use this file except in compliance with the License.
%
% You may obtain a copy of the License at
% http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing,
% software distributed under the License is distributed on an
% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
% either express or implied.
%
% See the License for the specific language governing permissions
% and limitations under the License.
%

-module(couch_lua_process).
-behaviour(gen_server).

-compile(export_all).

-export([
    start_link/0,
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    code_change/3,
    handle_info/2
]).
-export([set_timeout/2, prompt/2]).

-record(evstate, {
    ddocs = #{},
    funs = [],
    query_config = [],
    timeout = 5000,
    idle = 5000,
    lua_init,
    lua
}).

-record(lfun, {
    fchunk,
    lua
}).

-include_lib("couch/include/couch_db.hrl").
-include_lib("luerl/include/luerl.hrl").

start_link() ->
    gen_server:start_link(?MODULE, [], []).

% this is a bit messy, see also couch_query_servers handle_info
% stop(_Pid) ->
%     ok.

set_timeout(Pid, TimeOut) ->
    gen_server:call(Pid, {set_timeout, TimeOut}).

prompt(Pid, Data) when is_list(Data) ->
    gen_server:call(Pid, {prompt, Data}).

% gen_server callbacks
init([]) ->
    V = config:get("query_server_config", "os_process_idle_limit", "300"),
    Idle = list_to_integer(V) * 1000,
    St = #evstate{idle = Idle, lua_init = lua_init()},
    {ok, reset(St, []), Idle}.

handle_call({set_timeout, TimeOut}, _From, State) ->
    {reply, ok, State#evstate{timeout = TimeOut}, State#evstate.idle};
handle_call({prompt, Data}, _From, State) ->
    couch_log:debug("Prompt lua qs: ~s", [?JSON_ENCODE(Data)]),
    {NewState, Resp} =
        try run(State, to_binary(Data)) of
            {S, R} -> {S, R}
        catch
            throw:{error, Why} ->
                {State, [<<"error">>, Why, Why]}
        end,

    Idle = State#evstate.idle,
    case Resp of
        {error, Reason} ->
            Msg = io_lib:format("couch lua server error: ~p", [Reason]),
            Error = [<<"error">>, <<"lua_query_server">>, list_to_binary(Msg)],
            {reply, Error, NewState, Idle};
        [<<"error">> | Rest] ->
            % Msg = io_lib:format("couch lua server error: ~p", [Rest]),
            % TODO: markh? (jan)
            {reply, [<<"error">> | Rest], NewState, Idle};
        [<<"fatal">> | Rest] ->
            % Msg = io_lib:format("couch lua server error: ~p", [Rest]),
            % TODO: markh? (jan)
            {stop, fatal, [<<"error">> | Rest], NewState};
        Resp ->
            {reply, Resp, NewState, Idle}
    end.

handle_cast(garbage_collect, State) ->
    erlang:garbage_collect(),
    {noreply, State, State#evstate.idle};
handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(_Msg, State) ->
    {noreply, State, State#evstate.idle}.

handle_info(timeout, State) ->
    gen_server:cast(couch_proc_manager, {os_proc_idle, self()}),
    erlang:garbage_collect(),
    {noreply, State, State#evstate.idle};
handle_info({'EXIT', _, normal}, State) ->
    {noreply, State, State#evstate.idle};
handle_info({'EXIT', _, Reason}, State) ->
    {stop, Reason, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVersion, State, _Extra) ->
    {ok, State}.

run(#evstate{} = State, [<<"reset">>]) ->
    {reset(State, []), true};
run(#evstate{} = State, [<<"reset">>, QueryConfig]) ->
    {reset(State, QueryConfig), true};
run(#evstate{funs = Funs} = State, [<<"add_fun">>, BinFunc]) ->
    FunInfo = make_fun(State, BinFunc),
    {State#evstate{funs = Funs ++ [FunInfo]}, true};
run(State, [<<"map_doc">>, Doc]) ->
    Resp = lists:map(
        fun(#lfun{fchunk = FChunk, lua = Lua}) ->
            {Doc1, Lua1} = encode_ejson(Doc, Lua),
            {ok, _, Lua2} = luerl_new:call_chunk(FChunk, [Doc1], Lua1),
            decode_map_kvs(Lua2)
        end,
        State#evstate.funs
    ),
    {State, Resp};
run(State, [<<"reduce">>, Funs, KVs]) ->
    {Keys, Vals} =
        lists:foldl(
            fun([K, V], {KAcc, VAcc}) ->
                {[K | KAcc], [V | VAcc]}
            end,
            {[], []},
            KVs
        ),
    Keys2 = lists:reverse(Keys),
    Vals2 = lists:reverse(Vals),
    {State, catch reduce(State, Funs, Keys2, Vals2, false)};
run(State, [<<"rereduce">>, Funs, Vals]) ->
    {State, catch reduce(State, Funs, null, Vals, true)};
run(#evstate{ddocs = DDocs} = State, [<<"ddoc">>, <<"new">>, DDocId, DDoc]) ->
    {State#evstate{ddocs = DDocs#{DDocId => DDoc}}, true};
run(#evstate{ddocs = DDocs} = State, [<<"ddoc">>, DDocId | Rest]) ->
    DDoc = load_ddoc(DDocs, DDocId),
    ddoc(State, DDoc, Rest);
run(_, Unknown) ->
    couch_log:error("Lua Process: Unknown command: ~p~n", [Unknown]),
    throw({error, unknown_command}).

ddoc(State, {DDoc}, [FunPath, Args]) ->
    % load fun from the FunPath
    BFun = lists:foldl(
        fun
            (Key, {Props}) when is_list(Props) ->
                couch_util:get_value(Key, Props, nil);
            (_Key, Fun) when is_binary(Fun) ->
                Fun;
            (_Key, nil) ->
                throw({error, not_found});
            (_Key, _Fun) ->
                throw({error, malformed_ddoc})
        end,
        {DDoc},
        FunPath
    ),
    ddoc(State, make_fun(State, BFun), FunPath, Args).

ddoc(State, #lfun{} = LFun, [<<"validate_doc_update">>], Args) ->
    {State, apply_ddoc_fun(LFun, Args)};
ddoc(State, #lfun{} = LFun, [<<"rewrites">>], Args) ->
    {State, apply_ddoc_fun(LFun, Args)};
ddoc(State, #lfun{} = LFun, [<<"filters">> | _], [Docs, Req]) ->
    FilterFunWrapper = fun(Doc) ->
        case apply_ddoc_fun(LFun, [Doc, Req]) of
            true -> true;
            false -> false;
            {'EXIT', Error} -> couch_log:error("~p", [Error])
        end
    end,
    Resp = lists:map(FilterFunWrapper, Docs),
    {State, [true, Resp]};
ddoc(_State, #lfun{}, [<<"views">> | _], [_Docs]) ->
    throw({error, not_supported});
ddoc(_State, #lfun{}, [<<"shows">> | _], _Args) ->
    throw({error, not_supported});
ddoc(State, #lfun{} = LFun, [<<"updates">> | _], Args) ->
    Resp =
        case apply_ddoc_fun(LFun, Args) of
            [JsonDoc, JsonResp] ->
                [<<"up">>, JsonDoc, JsonResp]
        end,
    {State, Resp};
ddoc(_State, _, [<<"lists">> | _], _Args) ->
    throw({error, not_supported}).

apply_ddoc_fun(#lfun{fchunk = Chunk, lua = Lua}, Args0) ->
    {Args, Lua1} = encode_list(Args0, Lua),
    try luerl_new:call(Chunk, Args, Lua1) of
        {ok, Res, Lua2} ->
            decode_ejson(Res, Lua2);
        {lua_error, {error_call, LuaErr}, Lua2} ->
            decode_ejson(LuaErr, Lua2)
    catch
        error:Error:Stack ->
            {'EXIT', {Error, Stack}};
        exit:Exit ->
            {'EXIT', Exit};
        throw:Term ->
            Term
    end.

load_ddoc(#{} = DDocs, DDocId) ->
    try map_get(DDocId, DDocs) of
        {DDoc} -> {DDoc}
    catch
        _:_Else ->
            throw(
                {error,
                    ?l2b(io_lib:format("Lua Query Server missing DDoc with Id: ~s", [DDocId]))}
            )
    end.

make_fun(#evstate{lua = Lua}, Source) when is_binary(Source) ->
    Source1 = binary_to_list(Source),
    {ok, Chunk, Lua1} = luerl_new:load("f = " ++ Source1 ++ "\nreturn f(...)", Lua),
    #lfun{fchunk = Chunk, lua = Lua1}.

reduce(State, BinFuns, Keys, Vals, ReReduce) ->
    Funs =
        case is_list(BinFuns) of
            true ->
                lists:map(fun(BF) -> make_fun(State, BF) end, BinFuns);
            _ ->
                [make_fun(State, BinFuns)]
        end,
    RedFun = fun(#lfun{fchunk = Chunk, lua = Lua}) ->
        {Keys1, Lua1} = encode_list(Keys, Lua),
        {Vals1, Lua2} = encode_list(Vals, Lua1),
        {ok, Res, Lua3} = luerl_new:call(Chunk, [Keys1, Vals1, ReReduce], Lua2),
        decode_list(Res, Lua3)
    end,
    [true, lists:map(RedFun, Funs)].

to_binary({Data}) ->
    Pred = fun({Key, Value}) ->
        {to_binary(Key), to_binary(Value)}
    end,
    {lists:map(Pred, Data)};
to_binary(Data) when is_list(Data) ->
    [to_binary(D) || D <- Data];
to_binary(null) ->
    null;
to_binary(true) ->
    true;
to_binary(false) ->
    false;
to_binary(Data) when is_atom(Data) ->
    list_to_binary(atom_to_list(Data));
to_binary(Data) ->
    Data.

reset(#evstate{ddocs = DDocs, idle = Idle, lua_init = Lua}, QueryConfig) ->
    #evstate{
        ddocs = DDocs,
        idle = Idle,
        lua = Lua,
        lua_init = Lua,
        query_config = QueryConfig
    }.

lua_init() ->
    Lua0 = luerl_sandbox:init(),
    EmitF = "
null = {}
kvs = {}
function emit(k, v) table.insert(kvs, {k, v}) end
    ",
    {ok, _, Lua2} = luerl_new:do(EmitF, Lua0),
    Lua2.

%% encode_list([Term], State) -> {[LuerlTerm],State}.
%% encode_ejson(Term, State) -> {LuerlTerm,State}.

encode_list(Ts, St) ->
    lists:mapfoldl(fun encode_ejson/2, St, Ts).

encode_ejson(null, St) ->
    {ok, Null, _} = luerl_new:get_table_keys([<<"null">>], St),
    {Null, St};
encode_ejson(false, St) ->
    {false, St};
encode_ejson(true, St) ->
    {true, St};
encode_ejson(B, St) when is_binary(B) ->
    {B, St};
encode_ejson(A, St) when is_atom(A) ->
    {atom_to_binary(A, latin1), St};
encode_ejson(N, St) when is_number(N) ->
    {N, St};
encode_ejson(F, St) when is_map(F) ->
     encode_ejson({maps:to_list(F)}, St);
encode_ejson({L}, St0) when is_list(L) ->
    EncTab = fun({K0, V0}, S0) ->
        {K1, S1} = encode_ejson(K0, S0),
        {V1, S2} = encode_ejson(V0, S1),
        {{K1, V1}, S2}
    end,
    {Es, St1} = lists:mapfoldl(EncTab, St0, L),
    {T, St2} = luerl_heap:alloc_table(Es, St1),
    {T, St2};
encode_ejson(L, St0) when is_list(L) ->
    %% Encode the table elements in the list.
    EncTab = fun
        (V0, {I, S0}) ->
            {V1, S1} = encode_ejson(V0, S0),
            {{I, V1}, {I + 1, S1}}
    end,
    {Es0, {_, St1}} = lists:mapfoldl(EncTab, {1, St0}, L),
    % Use the luarocks hack - fill in table[0] slot with array
    % length so we can differentiate an array from an object
    Es = Es0 ++ [{0, length(L)}],
    {T, St2} = luerl_heap:alloc_table(Es, St1),
    {T, St2};
encode_ejson(_, _) ->
    error(badarg).

%% decode_list([LuerlTerm], State) -> [Term].
%% decode_ejson(LuerlTerm, State) -> Term.
%%  In decode we track of which tables we have seen to detect
%%  recursive references and generate an error when that occurs.

decode_list(Lts, St) ->
    lists:map(fun (Lt) -> decode_ejson(Lt, St) end, Lts).

decode_ejson(Lt, St) ->
    {ok, Null, _} = luerl_new:get_table_keys([<<"null">>], St),
    decode_ejson(Lt, St, #{}, Null).

decode_ejson(nil, _, _, _) ->
    nil;
decode_ejson(false, _, _, _) ->
    false;
decode_ejson(true, _, _, _) ->
    true;
decode_ejson(Null, _, _, Null) ->
    null;
decode_ejson(B, _, _, _) when is_binary(B) ->
    B;
decode_ejson(N, _, _, _) when is_number(N) ->
    N;
decode_ejson(#tref{} = T, St, In, Null) ->
    decode_table(T, St, In, Null);
decode_ejson(_, _, _, _) ->
    error(badarg).

decode_table(#tref{i = N} = T, St, In, Null) ->
    case is_map_key(N, In) of
        true ->
            error({recursive_table, T});
        false ->
            try
                decode_arr(T, St, In#{N => true}, Null)
            catch
                not_an_array ->
                    decode_obj(T, St, In#{N => true}, Null)
            end
    end.

decode_arr(T, St, In, Null) ->
    % Here since we're using foldr we're going backwords, so
    % indices should be decreasing all the way down to 0
    case luerl_heap:get_table(T, St) of
        #table{a = Arr, d = Dict} ->
            Fun = fun
                (0, V, {undefined, []}) when is_integer(V), V >= 0 ->
                    % The t[0] = arraylen [] roundtrip hack case
                    {V, []};
                (I, V, {undefined, []}) when is_integer(I), I >= 1 ->
                    % First element key is numeric, a good chance it's an array
                    {I - 1, [decode_ejson(V, St, In, Null)]};
                (I, V, {NextI, Acc}) when is_integer(I), I =:= NextI ->
                    {NextI - 1, [decode_ejson(V, St, In, Null) | Acc]};
                (_, _, {_, _}) ->
                    throw(not_an_array)
            end,
            Ts = ttdict:fold(Fun, {undefined, []}, Dict),
            {LastI, Res} = array:sparse_foldr(Fun, Ts, Arr),
            case {LastI, Res} of
                {0, []} ->
                    % The t[0] = arraylen [] roundtrip hack
                    [];
                {undefined, []} ->
                    % Regular empty table, we choose this to be an empty object
                    {[]};
                {0, [_ | _]} ->
                    % Got to the last index with an non-empty result
                    % This is the common/expected case for an array
                    Res;
                {_, _} ->
                    % Something other than the above cases. Probably an object
                    throw(not_an_array)
            end;
        _ ->
            error(badarg)
    end.

decode_obj(T, St, In, Null) ->
    case luerl_heap:get_table(T, St) of
        #table{a = Arr, d = Dict} ->
            Fun = fun(K, V, Acc) ->
                DecK = decode_ejson(K, St, In, Null),
                DecV = decode_ejson(V, St, In, Null),
                [{DecK, DecV} | Acc]
            end,
            Ts = ttdict:fold(Fun, [], Dict),
            ResProps = array:sparse_foldr(Fun, Ts, Arr),
            {ResProps};
        _ ->
            error(badarg)
    end.

decode_map_kvs(Lua) ->
    {ok, KVsTab, _} = luerl_new:get_table_keys([<<"kvs">>], Lua),
    case decode_ejson(KVsTab, Lua) of
        {[]} -> [];
        KVs = [[_, _] | _] -> KVs;
        _ -> error(badarg)
    end.

-ifdef(TEST).

-include_lib("couch/include/couch_eunit.hrl").

-define(TEST_TERMS, [
    true, false, null,
    0, 0.0, 1, -1, -0.1, 0.1, 1.0e300, -1.0e300, 1.0e-300, -1.0e-300, 1 bsl 64, -1 bsl 64,
    <<"a">>, <<"0">>, <<"false">>, <<"null">>,
    [], [0], [0, 0], [null, 0, true, <<"a">>, -0.1, null], [[]], [[], []],
    [[]], [[], []], [0, []], [true, [[null], 0.001], <<"a">>],
    {[]}, {[{<<"a">>, null}]}, {[{null, 100}]},
    [{[]}, null, {[]}, 0, [[[[[[42]]]]]]], [{[{<<"a">>, []}]}],
    {[{<<"a">>, {[{<<"b">>, {[{<<"c">>, 42}]}}]}}]}
]).

roundtrip_test_() ->
    [test_term(T) || T <- ?TEST_TERMS].

encode_decode_test() ->
    ?assertEqual({[]}, encdec(#{})),
    ?assertEqual({[{<<"a">>, null}]}, encdec(#{<<"a">> => null})).

test_term(T) ->
    Name = lists:flatten(io_lib:format("~p",[T])),
    {Name, ?_assertEqual(T, encdec(T))}.

encdec(T) ->
    S0 = lua_init(),
    {Es, S1} = encode_ejson(T, S0),
    decode_ejson(Es, S1).

-endif.
