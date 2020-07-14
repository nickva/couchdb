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

-module(couch_replicator_acceptor).


-include("couch_replicator.hrl").


-export([
   start/4,
   stop/1
]).


start(_Type, Count, _Parent, _Opts) when Count < 1 ->
    #{};

start(Type, Count, Parent, Opts) when Count > 1 ->
    lists:foldl(fun(_N, Acc) ->
        Pid = spawn_link(fun() -> accept(Type, Parent, Opts) end),
        Acc#{Pid => true}
    end, #{}, lists:seq(1, Max)).


stop(#{} = Acceptors) ->
    maps:map(fun(Pid, true) ->
        unlink(Pid),
        exit(Pid, kill)
    end, Acceptors),
    ok.


accept(Type, Parent, Opts) ->
    case couch_jobs:accept(Type, Opts) of
        {ok, Job, JobData} ->
            ok = gen_server:cast(Parent, {?ACCEPTED_JOB, Job, JobData});
        {error, not_found} ->
            ok
    end.
