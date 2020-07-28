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

-module(couch_replicator_job_server).


-behaviour(gen_server).


-export([
    start_link/0
]).

-export([
    accepted/2
]).

-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3
]).

-define(MAX_ACCEPTORS, 5).
-define(MAX_JOBS, 500).
-define(MAX_CHURN, 100).
-define(INTERVAL, 30).
-define(MIN_RUNTIME, 90).


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


% Callback used by acceptors after they accepted a job and become a worker
%
accepted(Worker, Normal) when is_pid(Worker), is_boolean(Normal) ->
    gen_server:call(?MODULE, {accepted, Worker, Normal}, infinity).


init(_) ->
    process_flag(trap_exit, true),
    couch_replicator_jobs:set_couch_jobs_timeout(),
    St = #{
        acceptors => #{},
        workers => #{},
        churn => 0,
        config => get_config()
    },
    do_send_after(St),
    {ok, spawn_acceptors(St)}.


terminate(_, _St) ->
    ok.


handle_call({accepted, Pid, Normal}, _From, #{} = St) ->
    #{
        acceptors := Acceptors,
        workers := Workers,
        churn := Churn
    } = St,
    case maps:is_key(Pid, Acceptors) of
        true ->
            St1 = St#{
                acceptors := maps:remove(Pid, Acceptors),
                Val = {Normal, erlang:system_time(second)},
                workers := Workers#{Pid => Val},
                churn := Churn + 1
            },
            {reply, ok, spawn_acceptors(St1)};
        false ->
            LogMsg = "~p : unknown acceptor processs ~p",
            couch_log:error(LogMsg, [?MODULE, Pid]),
            {stop, {unknown_acceptor_pid, Pid}, St}
    end;

handle_call(Msg, _From, St) ->
    {stop, {bad_call, Msg}, {bad_call, Msg}, St}.


handle_cast(Msg, St) ->
    {stop, {bad_cast, Msg}, St}.


handle_info(reschedule, #{} = St1) ->
    St2 = St1#{config := get_config()},
    St3 = trim_jobs(St2)
    St4 = reschedule(St3),
    St5 = do_send_after(St4),
    St6 = St5#{churn := 0},
    {ok, St6};

handle_info({'EXIT', Pid, Reason}, #{} = St) ->
    #{
        acceptors := Acceptors,
        workers := Workers
    } = St,
    case {maps:is_key(Pid, Acceptors), maps:is_key(Pid, Workers)} of
        {true, false} -> handle_acceptor_exit(St, Pid, Reason);
        {false, true} -> handle_worker_exit(St, Pid, Reason);
        {false, false} -> handle_unknown_exit(St, Pid, Reason)
    end;

handle_info(Msg, St) ->
    {stop, {bad_info, Msg}, St}.


code_change(_OldVsn, St, _Extra) ->
    {ok, St}.


% Scheduling logic

do_send_after(#{} = St) ->
    #{config := #{interval_sec := IntervalSec}} = St,
    IntervalMSec = IntervalSec * 1000,
    Jitter = IntervalMSec div 3,
    WaitMSec = IntervalMSec + rand:uniform(max(1, Jitter)),
    erlang:send_after(WaitMSec, self(), reschedule).


reschedule(#{} = St) ->
    #{
        churn := Churn,
        acceptors := Acceptors,
        workers := Workers,
        config := #{max_jobs := MaxJobs, max_churn := MaxChurn}
    } = St1,

    ACnt = maps:size(Acceptors),
    WCnt = maps:size(Workers),

    ChurnLeft = MaxChurn - Churn,
    Slots = (MaxJobs + MaxChurn) - (ACnt + WCnt),
    Pending = if min(Slots, ChurnLeft) =< 0 -> 0; true ->
        % Don't fetch pending if we don't have enough slots of churn budget
        couch_replicator_jobs:pending_count(undefined)
    end,

    % Start new acceptors only if we have churn budget, there are pending jobs
    % and we won't start more than max jobs + churn total acceptors
    ToStart = max(0, lists:min([ChurnLeft, Pending, Slots])),

    LogMsg = "~p : reschedule churn_left:~p pending:~p slots:~p to_start:~p",
    couch_log:debug(LogMsg, [?MODULE, ChurnLeft, Pending, Slots, ToStart]),

    lists:foldl(fun(_, StAcc) ->
        Pid = couch_replicator_scheduler_job:start_link(),
        StAcc#{acceptors := Acceptors#{Pid => true}}
    end, St1, lists:seq(1, NeedAcceptors)).


trim_jobs(#{} = St) ->
    #{
        workers := Workers,
        churn := Churn,
        config := #{max_jobs := MaxJobs},
    } = St,
    Excess = max(0, maps:size(Workers) - MaxJobs),
    lists:foreach(fun stop_job/1, stop_candidates(St, Excess)),
    St#{churn := Churn + Excess}.


stop_candidates(#{}, Top) when is_integer(Top), Top =< 0 ->
    [];

stop_candidates(#{} = St, Top) when is_integer(Top), Top > 0 ->
    #{
        workers := Workers,
        config := #{min_run_time := MinRunTime},
    } = St,

    WList1 = maps:to_list(Workers), % [{Pid, {Normal, StartTime}},...]

    % Filter out normal jobs and those which have just started running
    MaxT = erlang:system_time(second) - MinRunTime,
    WList2 = lists:filter(fun({_Pid, {Normal, T}}) ->
        not Normal andalso T < MaxT
    end, WList1),

    Sorted = lists:keysort(2, WList2),
    lists:map(fun({Pid, _}) -> Pid end, Sorted).


stop_job(Pid) when is_pid(Pid) ->
    % Replication jobs handle the shutdown signal and then checkpoint in
    % terminate handler
    exit(Pid, shutdown).


spawn_acceptors(St) ->
    #{
        workers := Workers,
        acceptors := Acceptors,
        config := #{max_jobs := MaxJobs, max_acceptors := MaxAcceptors}
    } = St,
    ACnt = maps:size(Acceptors),
    WCnt = maps:size(Workers),
    case ACnt < MaxAcceptors andalso (ACnt + WCnt) < MaxJobs of
        true ->
            Pid = couch_replicator_scheduler_job:start_link(),
            NewSt = St#{acceptors := Acceptors#{Pid => true}},
            spawn_acceptors(NewSt);
        false ->
            St
    end.


% Worker process exit handlers

handle_acceptor_exit(#{acceptors := Acceptors} = St, Pid, Reason) ->
    St1 = St#{acceptors := maps:remove(Pid, Acceptors)},
    LogMsg = "~p : acceptor process ~p exited with ~p",
    couch_log:error(LogMsg, [?MODULE, Pid, Reason]),
    {noreply, spawn_acceptors(St1)}.


handle_worker_exit(#{workers := Workers} = St, Pid, normal) ->
    St1 = St#{workers := maps:remove(Pid, Workers)},
    {noreply, spawn_acceptors(St1)};

handle_worker_exit(#{workers := Workers} = St, Pid, Reason) ->
    St1 = St#{workers := maps:remove(Pid, Workers)},
    LogMsg = "~p : replicator job process ~p exited with ~p",
    couch_log:error(LogMsg, [?MODULE, Pid, Reason]),
    {noreply, spawn_acceptors(St1)}.


handle_unknown_exit(St, Pid, Reason) ->
    LogMsg = "~p : unknown process ~p exited with ~p",
    couch_log:error(LogMsg, [?MODULE, Pid, Reason]),
    {stop, {unknown_pid_exit, Pid}, St}.


get_config() ->
    Defaults = #{
        max_acceptors => ?MAX_ACCEPTORS,
        interval_sec => ?INTERVAL_SEC,
        max_jobs => ?MAX_JOBS,
        max_churn => ?MAX_CHURN,
        min_run_time => ?MIN_RUN_TIME
    },
    maps:map(fun(K, Default) ->
        {K, config:get_integer("replicator", binary_to_list(K), Default)}
    end, Defaults).
