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

-module(couch_replicator_docs).

-export([
    parse_rep_doc/1,
    parse_rep_doc/2,
    parse_rep_db/3,
    parse_rep_doc_without_id/1,
    parse_rep_doc_without_id/2,
    before_doc_update/3,
    after_doc_read/2,
    ensure_rep_db_exists/0,
    ensure_rep_ddoc_exists/1,
    remove_state_fields/2,
    update_doc_completed/3,
    update_failed/3,
    update_rep_id/1,
    update_triggered/3,
    update_error/4
]).


-include_lib("couch/include/couch_db.hrl").
-include_lib("ibrowse/include/ibrowse.hrl").
-include_lib("mem3/include/mem3.hrl").
-include_lib("couch_replicator/include/couch_replicator_api_wrap.hrl").
-include("couch_replicator.hrl").
-include("couch_replicator_js_functions.hrl").

-import(couch_util, [
    get_value/2,
    get_value/3,
    to_binary/1
]).

-import(couch_replicator_utils, [
    get_json_value/2,
    get_json_value/3
]).


-define(REP_DB_NAME, <<"_replicator">>).
-define(OWNER, <<"owner">>).
-define(CTX, {user_ctx, #user_ctx{roles=[<<"_admin">>, <<"_replicator">>]}}).
-define(replace(L, K, V), lists:keystore(K, 1, L, {K, V})).

-define(DEFAULT_SOCK_OPTS, "[{keepalive, true}, {nodelay, false}]").
-define(VALID_SOCK_OPTS, [
    buffer, delay_send, exit_on_close, ipv6_v6only, keepalive, nodelay, recbuf,
    send_timeout, send_timout_close, sndbuf, priority, tos, tclass
]).

-define(CONFIG_DEFAULTS, [
    {"worker_processes",    "4",                fun list_to_integer/1},
    {"worker_batch_size",   "500",              fun list_to_integer/1},
    {"http_connections",    "20",               fun list_to_integer/1},
    {"connection_timeout",  "30000",            fun list_to_integer/1},
    {"retries_per_request", "5",                fun list_to_integer/1},
    {"use_checkpoints",     "true",             fun list_to_existing_atom/1},
    {"checkpoint_interval", "30000",            fun list_to_integer/1},
    {"socket_options",      ?DEFAULT_SOCK_OPTS, fun parse_sock_opts/1}
]).


remove_state_fields(DbName, DocId) ->
    update_rep_doc(DbName, DocId, [
        {<<"_replication_state">>, undefined},
        {<<"_replication_state_time">>, undefined},
        {<<"_replication_state_reason">>, undefined},
        {<<"_replication_id">>, undefined},
        {<<"_replication_stats">>, undefined}]).


-spec update_doc_completed(binary(), binary(), [_]) -> any().
update_doc_completed(DbName, DocId, Stats) ->
    update_rep_doc(DbName, DocId, [
        {<<"_replication_state">>, <<"completed">>},
        {<<"_replication_state_reason">>, undefined},
        {<<"_replication_stats">>, {Stats}}]),
    couch_stats:increment_counter([couch_replicator, docs,
        completed_state_updates]).


-spec update_failed(binary(), binary(), any()) -> any().
update_failed(DbName, DocId, Error) ->
    Reason = error_reason(Error),
    couch_log:error("Error processing replication doc `~s` from `~s`: ~s",
        [DocId, DbName, Reason]),
    update_rep_doc(DbName, DocId, [
        {<<"_replication_state">>, <<"failed">>},
        {<<"_replication_stats">>, undefined},
        {<<"_replication_state_reason">>, Reason}]),
    couch_stats:increment_counter([couch_replicator, docs,
        failed_state_updates]).


-spec update_triggered(binary(), binary(), binary()) -> ok.
update_triggered(Id, DocId, DbName) ->
    update_rep_doc(DbName, DocId, [
        {<<"_replication_state">>, <<"triggered">>},
        {<<"_replication_state_reason">>, undefined},
        {<<"_replication_id">>, Id},
        {<<"_replication_stats">>, undefined}]),
    ok.


-spec update_error(binary(), binary(), binary(), any()) -> ok.
update_error(RepId0, DbName, DocId, Error) ->
    Reason = error_reason(Error),
    RepId = case RepId0 of
        Id when is_binary(Id) -> Id;
        _Other -> null
    end,
    update_rep_doc(DbName, DocId, [
        {<<"_replication_state">>, <<"error">>},
        {<<"_replication_state_reason">>, Reason},
        {<<"_replication_stats">>, undefined},
        {<<"_replication_id">>, BinRepId}]),
    ok.


-spec ensure_rep_db_exists() -> ok.
ensure_rep_db_exists() ->
    Opts = [?CTX, sys_db],
    case fabric2_db:create(?REP_DB_NAME, [?CTX, sys_db]) of
        {error, file_exists} -> ok;
        {ok, _Db} -> ok
    end.


-spec parse_rep_doc_without_id({[_]}) -> #{}.
parse_rep_doc_without_id(RepDoc) ->
    {ok, Rep} = try
        parse_rep_doc_without_id(RepDoc, null)
    catch
        throw:{error, Reason} ->
            throw({bad_rep_doc, Reason});
        Tag:Err ->
            throw({bad_rep_doc, to_binary({Tag, Err})})
    end,
    Rep.


-spec parse_rep_doc({[_]}, user_name()) -> {ok, #{}}.
parse_rep_doc({[_]} = Doc, UserName) ->
    {ok, Rep} = parse_rep_doc_without_id(Doc, UserName),
    #{?OPTIONS := Options} = Rep,
    Cancel = maps:get(<<"cancel">>, Options, false),
    Id = maps:get(<<"id">>, Options, nil),
    case {Cancel, Id} of
        {true, nil} ->
            % Cancel request with no id, must parse id out of body contents
            {ok, update_rep_id(Rep)};
        {true, Id} ->
            % Cancel request with an id specified, so do not parse id from body
            {ok, Rep};
        {false, _Id} ->
            % Not a cancel request, regular replication doc
            {ok, update_rep_id(Rep)}
    end.


-spec parse_rep_doc_without_id({[_]} | #{}, user_name()) -> {ok, #{}}.
parse_rep_doc_without_id({[_]} = EJson, UserName) ->
    % Normalize all field names to be binaries and turn into a map
    Map = ?JSON_DECODE(?JSON_ENCODE(EJson)),
    parse_rep_doc_without_id(Map, UserName);

parse_rep_doc_without_id(#{} = Doc, UserName) ->
    {SrcProxy, TgtProxy} = parse_proxy_settings(Doc),
    Opts = make_options(Doc),
    Cancel = maps:get(<<"cancel">>, Opts, false),
    Id = maps:get(<<"id">>, Opts, nil),
    case Cancel andalso Id =/= nil of
    true ->
        {ok, #{?OPTIONS => Opts, ?REP_USER => UserName}};
    false ->
        #{?SOURCE := Source0, ?TARGET := Target0} = Doc,
        Source = parse_rep_db(Source0, SrcProxy, Opts),
        Target = parse_rep_db(Target0, TgtProxy, Opts),
        case couch_replicator_filters:view_type(Props, Opts) of
            {error, Error} ->
                throw({bad_request, Error});
            _ ->
                ok
        end,
        couch_replicator_filters:parse(Opts) of
            {ok, _} -> ok;
            {error, FilterError} -> throw({error, FilterError})
        end,
        Rep = #{
            ?REP_ID => null,
            ?BASE_ID => null,
            ?SOURCE => Source,
            ?TARGET => Target,
            ?OPTIONS => Opts,
            ?REP_USER => UserName,
            ?DOC_ID => maps:get(<<"_id">>, Doc, null),
            ?DB_NAME => null,
            ?DOC_STATE => null,
            ?START_TIME => erlang:system_time()
        },
        {ok, Rep}
    end.


parse_proxy_settings(#{} = Doc) ->
    Proxy = maps:get(?PROXY, Doc, <<>>),
    SrcProxy = maps:get(?SOURCE_PROXY, Doc, <<>>),
    TgtProxy = maps:get(?TARGET_PROXY, Doc, <<>>),

    case Proxy =/= <<>> of
        true when SrcProxy =/= <<>> ->
            Error = "`proxy` is mutually exclusive with `source_proxy`",
            throw({bad_request, Error});
        true when  TgtProxy =/= <<>> ->
            Error = "`proxy` is mutually exclusive with `target_proxy`",
            throw({bad_request, Error});
        true ->
            {parse_proxy_params(Proxy), parse_proxy_params(Proxy)};
        false ->
            {parse_proxy_params(SrcProxy), parse_proxy_params(TgtProxy)}
    end.


% Update a #rep{} record with a replication_id. Calculating the id might involve
% fetching a filter from the source db, and so it could fail intermetently.
% In case of a failure to fetch the filter this function will throw a
%  `{filter_fetch_error, Reason} exception.
update_rep_id(#{} = Rep) ->
    {BaseId, ExtId} = couch_replicator_ids:replication_id(Rep),
    RepId = erlang:iolist_to_binary([BaseId, ExtId]),
    Rep#{?REP_ID := RepId, ?BASE_ID := BaseId}.


update_rep_doc(RepDbName, RepDocId, KVs) ->
    update_rep_doc(RepDbName, RepDocId, KVs, 1).


update_rep_doc(RepDbName, RepDocId, KVs, Wait) when is_binary(RepDocId) ->
    try
        case open_rep_doc(RepDbName, RepDocId) of
            {ok, LastRepDoc} ->
                update_rep_doc(RepDbName, LastRepDoc, KVs, Wait * 2);
            _ ->
                ok
        end
    catch
        throw:conflict ->
            Msg = "Conflict when updating replication doc `~s`. Retrying.",
            couch_log:error(Msg, [RepDocId]),
            ok = timer:sleep(couch_rand:uniform(erlang:min(128, Wait)) * 100),
            update_rep_doc(RepDbName, RepDocId, KVs, Wait * 2)
    end;

update_rep_doc(RepDbName, #doc{body = {RepDocBody}} = RepDoc, KVs, _Try) ->
    NewRepDocBody = lists:foldl(
        fun({K, undefined}, Body) ->
                lists:keydelete(K, 1, Body);
           ({<<"_replication_state">> = K, State} = KV, Body) ->
                case get_json_value(K, Body) of
                State ->
                    Body;
                _ ->
                    Body1 = lists:keystore(K, 1, Body, KV),
                    Timestamp = couch_replicator_utils:iso8601(os:timestamp()),
                    lists:keystore(
                        <<"_replication_state_time">>, 1, Body1,
                        {<<"_replication_state_time">>, Timestamp})
                end;
            ({K, _V} = KV, Body) ->
                lists:keystore(K, 1, Body, KV)
        end,
        RepDocBody, KVs),
    case NewRepDocBody of
    RepDocBody ->
        ok;
    _ ->
        % Might not succeed - when the replication doc is deleted right
        % before this update (not an error, ignore).
        save_rep_doc(RepDbName, RepDoc#doc{body = {NewRepDocBody}})
    end.


open_rep_doc(DbName, DocId) ->
    try
        case fabric2_db:open(DbName, [?CTX, sys_db]) of
            {ok, Db} -> fabric2_db:open_doc(Db, DocId, [ejson_body]);
            Else -> Else
        end
    catch
        error:database_does_not_exist ->
            {not_found, database_does_not_exist}
    end.


save_rep_doc(DbName, Doc) ->
    {ok, Db} = fabric2_db:open(DbName, [?CTX, sys_db]),
    try
        fabric2_db:update_doc(Db, Doc, [])
    catch
        % User can accidently write a VDU which prevents _replicator from
        % updating replication documents. Avoid crashing replicator and thus
        % preventing all other replication jobs on the node from running.
        throw:{forbidden, Reason} ->
            Msg = "~p VDU function preventing doc update to ~s ~s ~p",
            couch_log:error(Msg, [?MODULE, DbName, Doc#doc.id, Reason]),
            {ok, forbidden}
    end.


-spec parse_rep_db(#{}, #{}, #{}) -> #{}.
parse_rep_db(#{} = Endpoint, #{} = ProxyParams, #{} = Options) ->
    ProxyURL = case ProxyParams of
       #{<<"proxy_url">> := PUrl} -> PUrl;
       _ -> null
    end,

    Url0 = maps:get(<<"url">>, Endpoint),
    Url = maybe_add_trailing_slash(Url0),

    AuthProps = maps:get(<<"auth">>, Endpoint, #{}),

    Headers0 = maps:get(<<"headers">>, Endpoint, #{}),
    DefaultHeaders = couch_replicator_utils:get_default_headers(),
    % For same keys values in second map override those in the first
    Headers = maps:merge(DefaultHeaders, Headers0),

    SockOpts = maps:get(<<"socket_options">>, Options, #{}),
    SockAndProxy = maps:merge(SockOpts, ProxyParams),

    SslParams = ssl_params(Url),

    #{
        <<"url">> => Url,
        <<"auth_props">> => AuthProps,
        <<"headers">> => Headers,
        <<"ibrowse_options">> => maps:merge(SslParams, SockAndProxy),
        <<"timeout">> => maps:get(<<"timeout">>, Options),
        <<"http_connections">> => maps:get(<<"http_connections">>, Options),
        <<"retries">> => maps:get(<<"retries">>, Options)
        <<"proxy_url">> => ProxyUrl
    }.


parse_rep_db(<<"http://", _/binary>> = Url, Proxy, Options) ->
    parse_rep_db(#{<<"url">> => Url}, Proxy, Options);

parse_rep_db(<<"https://", _/binary>> = Url, Proxy, Options) ->
    parse_rep_db(#{<<"url">> => Url}, Proxy, Options);

parse_rep_db(<<_/binary>>, _Proxy, _Options) ->
    throw({error, local_endpoints_not_supported});

parse_rep_db(undefined, _Proxy, _Options) ->
    throw({error, <<"Missing replicator database">>}).


-spec maybe_add_trailing_slash(binary()) -> binary().
maybe_add_trailing_slash(<<>>) ->
    <<>>;

maybe_add_trailing_slash(Url) when is_binary(Url) ->
    case binary:match(Url, <<"?">>) of
        nomatch ->
            case binary:last(Url) of
                $/  -> Url;
                _ -> <<Url/binary, "/">>;
        _ ->
            Url  % skip if there are query params
    end.


-spec make_options(#{}) -> #{}.
make_options(#{} = RepDoc) ->
    Options0 = maps:fold(fun convert_options/3, #{}, RepDoc)
    Options = check_options(Options0),
    ConfigOptions = lists:foldl(fun({K, Default, ConversionFun}, Acc) ->
        V = ConversionFun(config:get("replicator", K, Default)),
        Acc#{list_to_binary(K) => V}
    end, #{}, ?CONFIG_DEFAULTS),
    maps:merge(ConfigOptions, Options).


-spec convert_options(binary(), any(), #{}) -> #{}.
convert_options(<<"cancel">>, V, _Acc) when not is_boolean(V)->
    throw({bad_request, <<"parameter `cancel` must be a boolean">>});
convert_options(<<"cancel">>, V, Acc) ->
    Acc#{<<"cancel">> => V};
convert_options(IdOpt, V, Acc) when IdOpt =:= <<"_local_id">>;
        IdOpt =:= <<"replication_id">>; IdOpt =:= <<"id">> ->
    Acc#{<<"id">> => couch_replicator_ids:convert(V)};
convert_options(<<"create_target">>, V, _Acc) when not is_boolean(V)->
    throw({bad_request, <<"parameter `create_target` must be a boolean">>});
convert_options(<<"create_target">>, V, Acc) ->
    Acc#{<<"create_target">> => V};
convert_options(<<"create_target_params">>, V, _Acc) when not is_tuple(V) ->
    throw({bad_request,
        <<"parameter `create_target_params` must be a JSON object">>});
convert_options(<<"create_target_params">>, V, Acc) ->
    Acc#{<<"create_target_params">> => V};
convert_options(<<"continuous">>, V, Acc) when not is_boolean(V)->
    throw({bad_request, <<"parameter `continuous` must be a boolean">>});
convert_options(<<"continuous">>, V, Acc) ->
    Acc#{<<"continuous">> => V};
convert_options(<<"filter">>, V, Acc) ->
    Acc#{<<"filter">> => V};
convert_options(<<"query_params">>, V, Acc) ->
    Acc#{<<"query_params">> => V};
convert_options(<<"doc_ids">>, null, Acc) ->
    Acc;
convert_options(<<"doc_ids">>, V, _Acc) when not is_list(V) ->
    throw({bad_request, <<"parameter `doc_ids` must be an array">>});
convert_options(<<"doc_ids">>, V, Acc) ->
    % Ensure same behaviour as old replicator: accept a list of percent
    % encoded doc IDs.
    DocIds = lists:usort([?l2b(couch_httpd:unquote(Id)) || Id <- V]),
    Acc#{<<"doc_ids">> => DocIds};
convert_options(<<"selector">>, V, _Acc) when not is_tuple(V) ->
    throw({bad_request, <<"parameter `selector` must be a JSON object">>});
convert_options(<<"selector">>, V, Acc) ->
    Acc#{<<"selector">> => V};
convert_options(<<"worker_processes">>, V, Acc) ->
    Acc#{<<"worker_processes">> => couch_util:to_integer(V)};
convert_options(<<"worker_batch_size">>, V, Acc) ->
    Acc#{<<"worker_batch_size">> => couch_util:to_integer(V)};
convert_options(<<"http_connections">>, V, Acc) ->
    Acc#{<<"http_connections">> => couch_util:to_integer(V)};
convert_options(<<"connection_timeout">>, V, Acc) ->
    Acc#{<<"connection_timeout">> => couch_util:to_integer(V)};
convert_options(<<"retries_per_request">>, V, Acc) ->
    Acc#{<<"retries">> => couch_util:to_integer(V)};
convert_options(<<"socket_options">>, V, Acc) ->
    Acc#{<<"socket_options">> => parse_sock_opts(V)};
convert_options(<<"since_seq">>, V, Acc) ->
    Acc#{<<"since_seq">> => V};
convert_options(<<"use_checkpoints">>, V, Acc) when not is_boolean(V)->
    throw({bad_request, <<"parameter `use_checkpoints` must be a boolean">>});
convert_options(<<"use_checkpoints">>, V, Acc) ->
    Acc#{<<"use_checkpoints">> => V};
convert_options(<<"checkpoint_interval">>, V, Acc) ->
    Acc#{<<"checkpoint_interval">>, couch_util:to_integer(V)};
convert_options(_K, _V, Acc) -> % skip unknown option
    Acc.


-spec check_options(#{}) -> #{}.
check_options(Options) ->
    DocIds = maps:is_key(<<"doc_ids">>, Options),
    Filter = maps:is_key(<<"filter">>, Options),
    Selector = maps:is_key(<<"selector">>, Options),
    case {DocIds, Filter, Selector} of
        {false, false, false} -> Options;
        {false, false, _} -> Options;
        {false, _, false} -> Options;
        {_, false, false} -> Options;
        _ ->
            throw({bad_request,
                "`doc_ids`,`filter`,`selector` are mutually exclusive"})
    end.


parse_sock_opts(V) ->
    {ok, SocketOptions} = couch_util:parse_term(V),
    lists:foldl(fun
        ({K, V}, Acc) when is_atom(K) ->
            case lists:member(K, ?VALID_SOCKET_OPTIONS) of
                true -> Acc#{atom_to_binary(K) => V};
                false -> Acc
            end;
        (_, Acc) ->
            Acc
    end, #{}, SocketOptions).


-spec parse_proxy_params(binary() | #{}) -> #{}.
parse_proxy_params(<<>>) ->
    #{};
parse_proxy_params(ProxyUrl0) when is_binary(ProxyUrl0)->
    ProxyUrl = binary_to_list(ProxyUrl0),
    #url{
        host = Host,
        port = Port,
        username = User,
        password = Passwd,
        protocol = Protocol0
    } = ibrowse_lib:parse_url(ProxyUrl),
    Protocol = atom_to_binary(Protocol, utf8),
    case lists:member(Protocol, [<<"http">>, <<"https">>, <<"socks5">>]) of
        true ->
            atom_to_binary(Protocol, utf8);
        false ->
            Error = <<"Unsupported proxy protocol", Protocol/binary>>,
            throw({bad_request, Error})
    end,
    ProxyParams = #{
        <<"proxy_url">> => ProxyUrl,
        <<"proxy_protocol">> => Protocol,
        <<"proxy_host">> => list_to_binary(Host),
        <<"proxy_port">> => Port
    #},
    case is_list(User) andalso is_list(Passwd) of
        true ->
            ProxyParams#{
                <<"proxy_user">> => list_to_binary(User),
                <<"proxy_password">> => list_to_binary(Passwd)
            };
        false ->
            ProxyParams
    end.


-spec ssl_params(binary()) -> #{}.
ssl_params(Url) ->
    case ibrowse_lib:parse_url(binary_to_list(Url)) of
    #url{protocol = https} ->
        Depth = list_to_integer(
            config:get("replicator", "ssl_certificate_max_depth", "3")
        ),
        VerifyCerts = config:get("replicator", "verify_ssl_certificates"),
        CertFile = config:get("replicator", "cert_file", null),
        KeyFile = config:get("replicator", "key_file", null),
        Password = config:get("replicator", "password", null),
        VerifySslOptions = ssl_verify_options(VerifyCerts =:= "true"),
        SslOpts = maps:merge(VerifySslOptions, #{<<"depth">> => Depth}),
        SslOpts1 = case CertFile /= null andalso KeyFile /= null of
            true ->
                CertFileOpts = case Password of
                    null ->
                        #{
                            <<"certfile">> => list_to_binary(CertFile),
                            <<"keyfile">> => list_to_binary(KeyFile)
                        };
                    _ ->
                        #{
                            <<"certfile">> => list_to_binary(CertFile),
                            <<"keyfile">> => list_to_binary(KeyFile),
                            <<"password">> => list_to_binary(Password)
                        }
                end,
                maps:merge(SslOpts, CertFileOpts)
            false ->
                SslOpts
        end,
        #{<<"is_ssl">> => true, <<"ssl_options">> => SslOpts1};
    #url{protocol = http} ->
        #{}
    end.


-spec ssl_verify_options(true | false) -> [_].
ssl_verify_options(true) ->
    case config:get("replicator", "ssl_trusted_certificates_file", undefined) of
        undefined ->
            #{
                <<"verify">> => <<"verify_peer">>,
                <<"cacertfile">> => null
            };
        CAFile when is_list(CAFile) ->
            #{
                <<"verify">> => <<"verify_peer">>,
                <<"cacertfile">> => list_to_binary(CAFile)
            }
    end;

ssl_verify_options(false) ->
    #{
        <<"verify">> => <<"verify_none">>
    }.


-spec before_doc_update(#doc{}, Db::any(), couch_db:update_type()) -> #doc{}.
before_doc_update(#doc{id = <<?DESIGN_DOC_PREFIX, _/binary>>} = Doc, _Db, _UpdateType) ->
    Doc;
before_doc_update(#doc{body = {Body}} = Doc, Db, _UpdateType) ->
    #user_ctx{
       roles = Roles,
       name = Name
    } = fabric2_db:get_user_ctx(Db),
    IsReplicator = case lists:member(<<"_replicator">>, Roles),
    Doc1 = case IsReplicator of
    true ->
        Doc;
    false ->
        case couch_util:get_value(?OWNER, Body) of
        undefined ->
            Doc#doc{body = {?replace(Body, ?OWNER, Name)}};
        Name ->
            Doc;
        Other ->
            case (catch fabric2_db:check_is_admin(Db)) of
            ok when Other =:= null ->
                Doc#doc{body = {?replace(Body, ?OWNER, Name)}};
            ok ->
                Doc;
            _ ->
                throw({forbidden, <<"Can't update replication documents",
                    " from other users.">>})
            end
        end
    end,
    case IsReplicator orelse Doc1#doc.deleted of
        true ->
            ok;  % If replicator or deleting don't validate doc body
        false ->
            % Encode as a map and normalize all field names as binaries
            BodyStr = couch_util:json_encode(Doc1#doc.body),
            BodyMap = couch_util:json_decode(BodyStr, [return_maps]),
            couch_replicator_validate_doc:validate(BodyMap)
    end,
    Doc1.


-spec after_doc_read(#doc{}, Db::any()) -> #doc{}.
after_doc_read(#doc{id = <<?DESIGN_DOC_PREFIX, _/binary>>} = Doc, _Db) ->
    Doc;
after_doc_read(#doc{body = {Body}} = Doc, Db) ->
    #user_ctx{name = Name} = fabric2_db:get_user_ctx(Db),
    case (catch fabric2_db:check_is_admin(Db)) of
    ok ->
        Doc;
    _ ->
        case couch_util:get_value(?OWNER, Body) of
        Name ->
            Doc;
        _Other ->
            Source0 = couch_util:get_value(<<"source">>, Body),
            Target0 = couch_util:get_value(<<"target">>, Body),
            Source = strip_credentials(Source0),
            Target = strip_credentials(Target0),
            NewBody0 = ?replace(Body, <<"source">>, Source),
            NewBody = ?replace(NewBody0, <<"target">>, Target),
            #doc{revs = {Pos, [_ | Revs]}} = Doc,
            NewDoc = Doc#doc{body = {NewBody}, revs = {Pos - 1, Revs}},
            fabric2_db:new_revid(NewDoc)
        end
    end.


-spec strip_credentials(undefined) -> undefined;
    (binary()) -> binary();
    ({[_]}) -> {[_]}.
strip_credentials(undefined) ->
    undefined;
strip_credentials(Url) when is_binary(Url) ->
    re:replace(Url,
        "http(s)?://(?:[^:]+):[^@]+@(.*)$",
        "http\\1://\\2",
        [{return, binary}]);
strip_credentials({Props0}) ->
    Props1 = lists:keydelete(<<"headers">>, 1, Props0),
    % Strip "auth" just like headers, for replication plugins it can be a place
    % to stash credential that are not necessarily in headers
    Props2 = lists:keydelete(<<"auth">>, 1, Props1),
    {Props2}.


error_reason({shutdown, Error}) ->
    error_reason(Error);
error_reason({bad_rep_doc, Reason}) ->
    to_binary(Reason);
error_reason({error, {Error, Reason}})
  when is_atom(Error), is_binary(Reason) ->
    to_binary(io_lib:format("~s: ~s", [Error, Reason]));
error_reason({error, Reason}) ->
    to_binary(Reason);
error_reason(Reason) ->
    to_binary(Reason).


-ifdef(TEST).


-include_lib("couch/include/couch_eunit.hrl").


check_options_pass_values_test() ->
    ?assertEqual(check_options([]), []),
    ?assertEqual(check_options([baz, {other, fiz}]), [baz, {other, fiz}]),
    ?assertEqual(check_options([{doc_ids, x}]), [{doc_ids, x}]),
    ?assertEqual(check_options([{filter, x}]), [{filter, x}]),
    ?assertEqual(check_options([{selector, x}]), [{selector, x}]).


check_options_fail_values_test() ->
    ?assertThrow({bad_request, _},
        check_options([{doc_ids, x}, {filter, y}])),
    ?assertThrow({bad_request, _},
        check_options([{doc_ids, x}, {selector, y}])),
    ?assertThrow({bad_request, _},
        check_options([{filter, x}, {selector, y}])),
    ?assertThrow({bad_request, _},
        check_options([{doc_ids, x}, {selector, y}, {filter, z}])).


check_convert_options_pass_test() ->
    ?assertEqual([], convert_options([])),
    ?assertEqual([], convert_options([{<<"random">>, 42}])),
    ?assertEqual([{cancel, true}],
        convert_options([{<<"cancel">>, true}])),
    ?assertEqual([{create_target, true}],
        convert_options([{<<"create_target">>, true}])),
    ?assertEqual([{continuous, true}],
        convert_options([{<<"continuous">>, true}])),
    ?assertEqual([{doc_ids, [<<"id">>]}],
        convert_options([{<<"doc_ids">>, [<<"id">>]}])),
    ?assertEqual([{selector, {key, value}}],
        convert_options([{<<"selector">>, {key, value}}])).


check_convert_options_fail_test() ->
    ?assertThrow({bad_request, _},
        convert_options([{<<"cancel">>, <<"true">>}])),
    ?assertThrow({bad_request, _},
        convert_options([{<<"create_target">>, <<"true">>}])),
    ?assertThrow({bad_request, _},
        convert_options([{<<"continuous">>, <<"true">>}])),
    ?assertThrow({bad_request, _},
        convert_options([{<<"doc_ids">>, not_a_list}])),
    ?assertThrow({bad_request, _},
        convert_options([{<<"selector">>, [{key, value}]}])).

check_strip_credentials_test() ->
    [?assertEqual(Expected, strip_credentials(Body)) || {Expected, Body} <- [
        {
            undefined,
            undefined
        },
        {
            <<"https://remote_server/database">>,
            <<"https://foo:bar@remote_server/database">>
        },
        {
            {[{<<"_id">>, <<"foo">>}]},
            {[{<<"_id">>, <<"foo">>}, {<<"headers">>, <<"bar">>}]}
        },
        {
            {[{<<"_id">>, <<"foo">>}, {<<"other">>, <<"bar">>}]},
            {[{<<"_id">>, <<"foo">>}, {<<"other">>, <<"bar">>}]}
        },
        {
            {[{<<"_id">>, <<"foo">>}]},
            {[{<<"_id">>, <<"foo">>}, {<<"headers">>, <<"baz">>}]}
        },
        {
            {[{<<"_id">>, <<"foo">>}]},
            {[{<<"_id">>, <<"foo">>}, {<<"auth">>, <<"pluginsecret">>}]}
        }
    ]].


setup() ->
    DbName = ?tempdb(),
    {ok, Db} = fabric2_db:create(DbName, [?ADMIN_CTX]),
    create_vdu(Db),
    DbName.


teardown(DbName) when is_binary(DbName) ->
    fabric2_db:delete(DbName, [?ADMIN_CTX]),
    ok.


create_vdu(Db) ->
    VduFun = <<"function(newdoc, olddoc, userctx) {throw({'forbidden':'fail'})}">>,
    Doc = #doc{
        id = <<"_design/vdu">>,
        body = {[{<<"validate_doc_update">>, VduFun}]}
    },
    {ok, _} = fabric2_db:update_doc(Db, [Doc]),
    ok.


update_replicator_doc_with_bad_vdu_test_() ->
    {
        setup,
        fun test_util:start_couch/0,
        fun test_util:stop_couch/1,
        {
            foreach, fun setup/0, fun teardown/1,
            [
                fun t_vdu_does_not_crash_on_save/1
            ]
        }
    }.


t_vdu_does_not_crash_on_save(DbName) ->
    ?_test(begin
        Doc = #doc{id = <<"some_id">>, body = {[{<<"foo">>, 42}]}},
        ?assertEqual({ok, forbidden}, save_rep_doc(DbName, Doc))
    end).


local_replication_endpoint_error_test_() ->
     {
        foreach,
        fun () -> meck:expect(config, get,
            fun(_, _, Default) -> Default end)
        end,
        fun (_) -> meck:unload() end,
        [
            t_error_on_local_endpoint()
        ]
    }.


t_error_on_local_endpoint() ->
    ?_test(begin
        RepDoc = {[
            {<<"_id">>, <<"someid">>},
            {<<"source">>, <<"localdb">>},
            {<<"target">>, <<"http://somehost.local/tgt">>}
        ]},
        Expect = local_endpoints_not_supported,
        ?assertThrow({bad_rep_doc, Expect}, parse_rep_doc_without_id(RepDoc))
    end).

-endif.
