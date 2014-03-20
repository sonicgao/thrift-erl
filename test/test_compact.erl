-module(test_compact).

-include("compact_types.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

nested_struct_in_list_test() ->
  A = [#aB{a = <<"ABC">>, b = <<"EFG">>}, #aB{a = <<"HIJ">>, b = <<"PLO">>}],
  B = #cD{c = A, d = 1234},

  C = thrift_decode(thrift_encode(B, 'cD'), 'cD'),
  ?assertEqual(C, B).

 
thrift_encode(Data, Struct) ->
    {ok, Trans} = thrift_memory_buffer:new(),
    {ok, Protocol} = thrift_binary_protocol:new(Trans),
    {Protocol2, ok} = thrift_protocol:write(Protocol, 
					    { compact_types:struct_info(Struct),  Data}),
    {_, Data2} = thrift_protocol:flush_transport(Protocol2),
    Data2.

thrift_decode(Data, Struct) ->
    {ok, Trans} = thrift_memory_buffer:new(Data),
    {ok, Protocol} = thrift_binary_protocol:new(Trans),
    {_, {ok, Resp}} = thrift_protocol:read(Protocol, 
					     compact_types:struct_info(Struct), Struct),
    Resp.

  



-endif.
