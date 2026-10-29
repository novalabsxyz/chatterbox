-module(client_stream_SUITE).

-include("http2.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-compile([export_all]).

%% Drives a client h2_stream directly, so the order its events arrive in
%% is the test's to choose rather than the scheduler's.

all() ->
    [
     response_before_send_h,
     send_data_before_send_h
    ].

init_per_testcase(_, Config) ->
    {ok, Listen} = gen_tcp:listen(0, [binary, {active, false}]),
    {ok, Port} = inet:port(Listen),
    {ok, Client} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    {ok, Peer} = gen_tcp:accept(Listen),
    ok = gen_tcp:close(Listen),
    Streams = h2_stream_set:new(client, {gen_tcp, Client}, ?MODULE, [], true),
    {ok, Pid} = h2_stream:start_link(1, Streams, self(), ?MODULE, [self()]),
    [{client, Client}, {peer, Peer}, {stream, Pid} | Config].

end_per_testcase(_, Config) ->
    Pid = ?config(stream, Config),
    unlink(Pid),
    exit(Pid, kill),
    gen_tcp:close(?config(client, Config)),
    gen_tcp:close(?config(peer, Config)),
    ok.

%% The connection process casts send_h only after writing HEADERS to the
%% socket, and the peer's frames come from the reader process, so a fast
%% peer's response can reach the stream first. It must wait for send_h,
%% not be taken as a server receiving a request and reset.
response_before_send_h(Config) ->
    Pid = ?config(stream, Config),
    h2_stream:send_event(Pid, {recv_h, response_headers()}),
    h2_stream:send_event(Pid, {recv_data, data_frame(<<"hello">>, 0)}),
    h2_stream:send_event(Pid, {send_h, request_headers()}),

    ?assertEqual({headers, response_headers()}, wait_callback()),
    ?assertEqual({data, <<"hello">>}, wait_callback()),
    ?assertMatch({open, _}, sys:get_state(Pid)),
    %% Nothing written: no RST_STREAM.
    ?assertEqual({error, timeout}, gen_tcp:recv(?config(peer, Config), 0, 100)),
    ok.

%% A sender that got the stream from the connection can race send_h too.
%% Its DATA must go out once the stream opens, not vanish into handle_info.
send_data_before_send_h(Config) ->
    Pid = ?config(stream, Config),
    Frame = data_frame(<<"hello">>, 0),
    h2_stream:send_data(Pid, Frame),
    h2_stream:send_event(Pid, {send_h, request_headers()}),

    Bin = iolist_to_binary(h2_frame:to_binary(Frame)),
    ?assertEqual({ok, Bin}, gen_tcp:recv(?config(peer, Config), byte_size(Bin), 1000)),
    ?assertMatch({open, _}, sys:get_state(Pid)),
    ok.

request_headers() ->
    [{<<":method">>, <<"POST">>},
     {<<":path">>, <<"/">>},
     {<<":scheme">>, <<"http">>},
     {<<":authority">>, <<"localhost">>}].

response_headers() ->
    [{<<":status">>, <<"200">>}].

data_frame(Bin, Flags) ->
    {#frame_header{type=?DATA, flags=Flags, length=byte_size(Bin), stream_id=1},
     h2_frame_data:new(Bin)}.

wait_callback() ->
    receive
        {?MODULE, Event} -> Event
    after 1000 ->
        timeout
    end.

%% h2_stream callbacks: report to the test process.

init(_Streams, _StreamId, [_Socket, TestPid]) ->
    {ok, TestPid}.

on_receive_headers(Headers, TestPid) ->
    TestPid ! {?MODULE, {headers, Headers}},
    {ok, TestPid}.

on_receive_data(Data, TestPid) ->
    TestPid ! {?MODULE, {data, iolist_to_binary(Data)}},
    {ok, TestPid}.
