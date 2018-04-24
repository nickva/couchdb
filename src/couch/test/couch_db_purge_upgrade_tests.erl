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

-module(couch_db_purge_upgrade_tests).

-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch/include/couch_db.hrl").

-define(CONTENT_JSON, {"Content-Type", "application/json"}).
-define(TIMEOUT, 1000).


setup() ->
    DbName = <<"db_with_1_purge_req">>,
    DbFileName = "db_with_1_purge_req.couch",
    OldDbFilePath = filename:join([?FIXTURESDIR, DbFileName]),
    DbDir = config:get("couchdb", "database_dir"),
    NewDbFilePath = filename:join([DbDir, DbFileName]),
    Files = [NewDbFilePath],

    %% make sure there is no left over
    lists:foreach(fun(File) -> file:delete(File) end, Files),
    file:copy(OldDbFilePath, NewDbFilePath),
    {DbName, Files}.


teardown({_DbName, Files}) ->
    lists:foreach(fun(File) -> file:delete(File) end, Files).


purge_upgrade_test_() ->
    {
        "Purge Upgrade tests",
        {
            setup,
            fun test_util:start_couch/0, fun test_util:stop_couch/1,
            {
                foreach,
                fun setup/0, fun teardown/1,
                [
                    %fun should_upgrade_legacy_db_with_0_purge_req/1,
                    %fun should_upgrade_legacy_db_with_1_purge_req/1
                    %fun should_upgrade_legacy_db_with_N_purge_req/1
                ]
            }
        }
    }.


should_upgrade_legacy_db_with_1_purge_req({DbName, Files}) ->
    ?_test(begin
        [_NewDbFilePath] = Files,
        ok = config:set("query_server_config", "commit_freq", "0", false),
        % add doc to trigger update
        DocUrl = db_url(DbName) ++ "/boo",
        {ok, Status, _Resp, _Body}  = test_request:put(
            DocUrl, [{"Content-Type", "application/json"}], <<"{\"a\":3}">>),
        ?assert(Status =:= 201 orelse Status =:= 202)
    end).


db_url(DbName) ->
    Addr = config:get("httpd", "bind_address", "127.0.0.1"),
    Port = integer_to_list(mochiweb_socket_server:get(couch_httpd, port)),
    "http://" ++ Addr ++ ":" ++ Port ++ "/" ++ ?b2l(DbName).
