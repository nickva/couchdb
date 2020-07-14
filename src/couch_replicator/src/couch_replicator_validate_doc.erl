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

-module(couch_replicator_validate_doc).


-include("couch_replicator.hrl").


-export([
    validate/2
]).


validate(#{<<"_replication_state">> := ?ST_FAILED}) ->
    % If replication failed, allow updating even a malformed document
    ok;

validate(#{?SOURCE := _, ?TARGET := _} = Doc) ->
    maps:fold(fun validate_field/2, Doc).
    validate_mutually_exclusive_filters(Doc);

validate(_) ->
    fail("Both `source` and `target` fields must exist").


validate_field(?SOURCE, V) when is_binary(V) ->
    ok;
validate_field(?SOURCE, #{?URL := _} = V) ->
    maps:fold(fun validate_endpoint_field/2, V);
validate_field(?SOURCE, _V) ->
    fail("`source` must be a string or an object with an `url` field").

validate_field(?TARGET, V) when is_binary(V) ->
    ok;
validate_field(?TARGET, #{?URL := _} = V) ->
    maps:fold(fun validate_endpoint_field/2, V);
validate_field(?TARGET, _V) ->
    fail("`target` must be a string or an object with an `url` field").

validate_field(?CONTINUOUS, V) when is_boolean(V) ->
    ok;
validate_field(?CONTINUOUS, _V) ->
    fail("`continuous` should be a boolean");

validate_field(?CREATE_TARGET, V) when is_boolean(V) ->
    ok;
validate_field(?CREATE_TARGET, _V) ->
    fail("`create_target` should be a boolean");

validate_field(?DOC_IDS, V) when is_list(V) ->
    lists:foreach(fun(DocId) ->
        case is_binary(DocId), byte_size(V) > 1 of
            true -> ok;
            false -> fail("`doc_ids` should be a list of strings")
        end
    end, V);
validate_field(?DOC_IDS, _V) ->
    fail("`doc_ids` should be a list of string");

validate_field(?SELECTOR, #{} = _V) ->
    ok;
validate_field(?SELECTOR, _V) ->
    fail("`selector` should be an object");

validate_field(?FILTER, V) when is_binary(V), byte_size(V) > 1 ->
    ok;
validate_field(?FILTER, _V) ->
    fail("`filter` should be a non-empty string");

validate_field(?QUERY_PARAMS, V) when is_map(V) orelse V =:= null ->
    ok;
validate_field(?QUERY_PARAMS, _V) ->
    fail("`query_params` should be a an object or `null`").


validate_endpoint_field(?URL, V) when is_binary(V) ->
    ok;
validate_endpoint_field(?URL, _V) ->
    fail("`url` endpoint field must be a string");

validate_endpoint_field(?AUTH, #{} = V) ->
    ok;
validate_endpoint_field(?AUTH, _V) ->
    fail("`auth` endpoint field must be an object");

validate_endpoint_field(?HEADERS, #{} = V) ->
    ok;
validate_endpoint_field(?HEADERS, _V) ->
    fail("`headers` endpoint field must be an object").


validate_mutually_exclusive_filter(#{} = Doc) ->
    DocIds = maps:get(?DOC_IDS, Doc, undefined),
    Selector = maps:get(?SELECTOR, Doc, undefined),
    Filter = maps:get(?FILTER, Doc, undefined),
    Defined = [V || V <- [DocIds, Selector, Filter], V =/= undefined],
    case length(Defined) > 1 of
        true ->
            fail("`doc_ids`, `selector` and `filter` are mutually exclusive");
        false ->
            ok
    end.


fail(Msg) when is_list(Msg) ->
    fail(list_to_binary(Msg));

fail(Msg) when is_binary(Msg) ->
    throw({forbidden, Msg}).
