-module(thrift_compact_protocol).
-behaviour(thrift_protocol).

-compile(export_all).

-include("thrift_constants.hrl").
-include("thrift_protocol.hrl").


%%% API
-export([
  new/1, new/2,
  flush_transport/1,
  close_transport/1
]).

%%% Callbacks
-export([
  new_protocol_factory/2,
  read/2,
  write/2
]).


-record(compact_protocol, {
  transport,
  strict_read,
  strict_write,
  % If we encounter a boolean field begin, save the TField here so it can
  % have the value incorporated.
  boolean_field,
  % If we read a field header, and it's a boolean field, save the boolean
  % value here so that readBool can use it.
  boolean_value,
  % Used to keep track of the last field for the current and previous structs,
  % so we can do the delta stuff
  last_field_stack = [],
  last_field_id = 0
}).

-type state() :: #compact_protocol{}.

-include("thrift_protocol_behaviour.hrl").

-define(PROTOCOL_ID, 16#82).
-define(VERSION, 1).
-define(VERSION_MASK, 16#1f).
-define(TYPE_MASK, 16#e0).
-define(TYPE_SHIFT_AMOUNT, 5).


%%% All of the on-wire type codes.
-define(Types_BOOLEAN_TRUE, 1).
-define(Types_BOOLEAN_FALSE, 2).
-define(Types_BYTE, 3).
-define(Types_I16, 4).
-define(Types_I32, 5).
-define(Types_I64, 6).
-define(Types_DOUBLE, 7).
-define(Types_BINARY, 8).
-define(Types_LIST, 9).
-define(Types_SET, 10).
-define(Types_MAP, 11).
-define(Types_STRUCT, 12).

-define(ANONYMOUS_STRUCT, #protocol_struct_begin{name = ""}).
-define(TSTOP, #protocol_field_begin{name = "", type = ?tType_STOP, id = 0}).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%

new(Transport) ->
  new(Transport, _Options = []).

new(Transport, Options) ->
  State0 = #compact_protocol{transport = Transport},
  State = parse_options(Options, State0),
  thrift_protocol:new(?MODULE, State).

flush_transport(This = #compact_protocol{transport = Transport}) ->
  {NewTransport, Result} = thrift_transport:flush(Transport),
  {This#compact_protocol{transport = NewTransport}, Result}.

close_transport(This = #compact_protocol{transport = Transport}) ->
  {NewTransport, Result} = thrift_transport:close(Transport),
  {This#compact_protocol{transport = NewTransport}, Result}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Instance methods
%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%% Write message
write(This, #protocol_message_begin{
    name = Name,
    type = Type,
    seqid = SeqId}) ->
  %lager:debug("write(~p, ~p, ~p)", [Name, Type, SeqId]),
  List = [
    {byte, ?PROTOCOL_ID},
    {byte, (?VERSION band ?VERSION_MASK) bor ((Type bsl ?TYPE_SHIFT_AMOUNT) band ?TYPE_MASK)},
    {varint, SeqId},
    {string, Name}],
  write_many(This, List);

write(This, message_end) -> {This, ok};

%%% Write struct
write(This=#compact_protocol{
            last_field_stack = LastFields,
            last_field_id = LastFieldId},
          _In=#protocol_struct_begin{}) ->
  %lager:debug("write(~p)", [_In]),
  NewLastFields = stack_push(LastFieldId, LastFields),
  NewLastFieldId = 0,
  {This#compact_protocol{last_field_stack=NewLastFields, last_field_id=NewLastFieldId}, ok};
write(This=#compact_protocol{
             last_field_stack = LastFields},
           _In=struct_end) ->
  %lager:debug("write(~p)", [_In]),
  {NewLastFieldId, NewLastFields} = stack_pop(LastFields),
  {This#compact_protocol{
    last_field_stack = NewLastFields,
    last_field_id = NewLastFieldId}, ok};

%%% Write field
write(This, Field=#protocol_field_begin{type = Type}) when Type =:= ?tType_BOOL ->
  {This#compact_protocol{boolean_field = Field}, ok};
write(This, Field=#protocol_field_begin{}) ->
  %lager:debug("write_field(~p)", [Field]),
  write_field_begin_internal(This, Field, -1);
  %write_many(This, [{byte, Type}, {i16, Id}]);

write(This, field_stop) -> write(This, {byte, ?tType_STOP});
write(This, field_end) -> {This, ok};

%%% Write map
write(This, #protocol_map_begin{
    ktype = _KType,
    vtype = _VType,
    size = 0}) ->
  write(This, {byte, 0});
write(This, #protocol_map_begin{
    ktype = KType,
    vtype = VType,
    size = Size}) ->
  %lager:info("MAP SIZE: ~p", [Size]),
  TwoTypes = (get_compact_type(KType) bsl 4) bor get_compact_type(VType),
  write_many(This, [{varint, Size}, {byte, TwoTypes}]);

write(This, map_end) ->
  {This, ok};

%%% Write boolean
write(This=#compact_protocol{
      boolean_field = undefined
    }, {bool, Value}) ->
  OutVal = case Value of
    true -> ?Types_BOOLEAN_TRUE;
    false -> ?Types_BOOLEAN_FALSE
  end,
  write(This, {byte, OutVal});
write(This=#compact_protocol{
      boolean_field = BoolField
    }, {bool, Value}) ->
  OutType = case Value of
    true -> ?Types_BOOLEAN_TRUE;
    false -> ?Types_BOOLEAN_FALSE
  end,
  {This1, Ret} = write_field_begin_internal(This, BoolField, OutType),
  {This1#compact_protocol{
    boolean_field = undefined
  }, Ret};

%%% Write list
write(This, #protocol_list_begin{
    etype = EType,
    size = Size}) ->
  write_collection_begin(This, EType, Size);

write(This, list_end) ->
  {This, ok};

%%% Write set
write(This, #protocol_set_begin{
    etype = EType,
    size = Size}) ->
  write_collection_begin(This, EType, Size);

write(This, set_end) ->
  {This, ok};

%%% Write numbers
write(This, {byte, Byte}) ->
  %lager:debug("write_byte(~p)", [Byte]),
  write(This, <<Byte:8/big-signed>>);

write(This, {i16, Short}) ->
  %lager:debug("write_short(~p)", [Short]),
  Num = number_to_zigzag(Short, i16),
  write(This, {varint, Num});

write(This, {i32, Int}) ->
  %lager:debug("write_int(~p)", [Int]),
  Num = number_to_zigzag(Int, i32),
  write(This, {varint, Num});

write(This, {i64, Long}) ->
  %lager:debug("write_long(~p)", [Long]),
  Num = number_to_zigzag(Long, i64),
  write(This, {varint, Num});

write(This, {varint, Num}) ->
  %lager:debug("write_varint: ~p", [Num]),
  write(This, encode_varint(Num));

%%% Write doubles
write(This, {double, Double}) ->
  write(This, <<Double:64/little-signed-float>>);

%%% Write strings
write(This, {string, Str}) when is_list(Str) ->
  write(This, {string, list_to_binary(Str)});

write(This, {string, Bin}) when is_binary(Bin) ->
  %lager:debug("write_string(~p, ~p, ~p)", [Bin, size(Bin), number_to_zigzag(size(Bin), i32)]),
  write_many(This, [{varint, size(Bin)}, Bin]);

%%% Write iolist
write(This = #compact_protocol{transport = Trans}, Data) ->
  %lager:debug("write_iolist: ~p", [Data]),
  {NewTransport, Result} = thrift_transport:write(Trans, Data),
  {This#compact_protocol{transport = NewTransport}, Result}.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Read
%%%%%%%%%%%%%%%%%%%%%%%%%%%%


read(This, message_begin) ->
  {This1, ProtocolId} = read(This, byte),
  SProtocol = get_sbyte(?PROTOCOL_ID),
  case ProtocolId of
    {ok, SProtocol} ->
      {This2, {ok, VerAndType}} = read(This1, byte),
      case VerAndType band ?VERSION_MASK of
        ?VERSION ->
          Type = (VerAndType bsr ?TYPE_SHIFT_AMOUNT) band 3,
          {This3, {ok, SeqId}} = read(This2, varint),
          {This4, MsgName} = read(This3, string),
          {This4, #protocol_message_begin{
            name = MsgName,
            type = Type,
            seqid = SeqId
          }};
        {ok, Other} ->
          {This2, {error, {bad_compact_protocol_version, [Other, ?VERSION]}}};
        Other ->
          {This2, {error, {bad_compact_protocol_read, Other}}}
      end;
    {ok, Other} ->
      {This1, {error, {bad_compact_protocol_id, [Other, ?PROTOCOL_ID]}}};
    Other ->
      {This1, {error, {bad_compact_protocol_read, Other}}}
  end;

read(This, message_end) ->
  {This, ok};

read(This=#compact_protocol{
      last_field_stack = LastFields,
      last_field_id = LastFieldId},
    struct_begin) ->
  NewLastFields = stack_push(LastFieldId, LastFields),
  %lager:debug("read_struct_begin: LastFields=~p, NewLastFields=~p", [LastFields, NewLastFields]),
  %lager:debug("read_struct_begin: LastFieldId=~p", [LastFieldId]),
  {This#compact_protocol{
    last_field_stack = NewLastFields,
    last_field_id = 0},
    ok};

read(This=#compact_protocol{
      last_field_stack = LastFields
    }, struct_end) ->
  {NewLastFieldId, NewLastFields} = stack_pop(LastFields),
  %lager:debug("read_struct_end: LastFields=~p, NewLastFields=~p", [LastFields, NewLastFields]),
  %lager:debug("read_struct_end: NewLastFieldId=~p", [NewLastFieldId]),
  {This#compact_protocol{
    last_field_stack = NewLastFields,
    last_field_id = NewLastFieldId}, ok};


read(This=#compact_protocol{
    last_field_id = LastFieldId,
    boolean_value = BoolVal
}, field_begin) ->
  {This1, Type} = read(This, byte),
  case Type of
    {ok, ?tType_STOP} ->
      %lager:debug("read_field_start: STOP Type=~p", [?TSTOP]),
      {This1, ?TSTOP};
    {ok, OtherType} ->
      % mask off the 4 MSB of the type header. it could contain a field id delta
      Modifier = ((OtherType band 16#f0) bsr 4),
      {This3, FieldId} = case Modifier of
                           0 ->
                             %lager:debug("Modifier = 0"),
                             {This2, {ok, FID}} = read(This1, i16),
                             {This2, FID};
                           _Else ->
                             %lager:debug("Modifier ~p, ~p", [Modifier, LastFieldId+Modifier]),
                             {This1, LastFieldId+Modifier}
                         end,
      Field = #protocol_field_begin{name = "", type = get_ttype(OtherType band 16#0f), id = FieldId},
      NewBoolVal = case is_bool_type(OtherType) of
                     true ->
                       case OtherType band 16#0f of
                         ?Types_BOOLEAN_TRUE -> true;
                         _ -> false
                       end;
                     _ ->
                       BoolVal
                   end,
      %lager:debug("read_field_start: ~p, ~p", [Field, NewBoolVal]),
      {This3#compact_protocol{
        last_field_id = FieldId,
        boolean_value = NewBoolVal
      }, Field}
  end;

read(This, field_end) ->
  {This, ok};

read(This, map_begin) ->
  %lager:debug("read_map"),
  {This1, Initial} = read(This, varint),
  case Initial of
    {ok, Size} ->
      {This3, KVType} = case Size of
        0 ->
          {This1, 0};
        _ ->
          {This2, {ok, KV}} = read(This1, byte),
          {This2, KV}
      end,
      %lager:info("K ~p V ~p S ~p", [KVType bsr 4, KVType band 16#0f, Size]),
      {This3, #protocol_map_begin{
        ktype = get_ttype(KVType bsr 4),
        vtype = get_ttype(KVType band 16#0f),
        size = Size
      }};
    Other ->
      {This1, Other}
  end;

read(This, map_end) ->
  {This, ok};

read(This, list_begin) ->
  {This1, Initial} = read(This, byte),
  case Initial of
    {ok, ST} ->
      {This3, Size} = case (ST bsr 4) band 16#0f of
                           15 ->
                             {This2, {ok, NS}} = read(This1, varint),
                             {This2, NS};
                           Other ->
                             {This1, Other}
                         end,
      Type = get_ttype(ST),
      {This3, #protocol_list_begin{
        etype = Type,
        size = Size
      }};
    Other ->
      {This1, Other}
  end;

read(This, list_end) ->
  {This, ok};

read(This, set_begin) ->
  {This1, List} = read(This, list_begin),
  %lager:debug("read_set: ~p", [List]),
  case List of
    #protocol_list_begin{} ->
      Type = List#protocol_list_begin.etype,
      Size = List#protocol_list_begin.size,
      {This1, #protocol_set_begin{
        etype = Type,
        size = Size
      }};
    Other ->
      {This1, Other}
  end;
read(This, set_end) ->
  {This, ok};

read(This, string) ->
  %lager:debug("read_string"),
  {This1, Initial} = read_varint(This),
  case Initial of
    {ok, Len} ->
      read_binary(This1, Len);
    Other ->
      {This1, Other}
  end;

read(This, binary) ->
  %lager:debug("read_binary"),
  {This1, Initial} = read_varint(This),
  case Initial of
    {ok, Len} ->
      read_binary(This1, Len);
    Other ->
      {This1, Other}
  end;

read(This=#compact_protocol{
      boolean_value = BoolValue
    }, bool) ->
  %lager:debug("read_bool: ~p", [BoolValue]),
  case BoolValue of
    undefined ->
      {This1, {ok, V}} = read(This, byte),
      {This1, {ok, V == ?Types_BOOLEAN_TRUE}};
    _ ->
      {This#compact_protocol{
        boolean_value = undefined
      }, {ok, BoolValue}}
  end;


read(This, byte) ->
  {This1, Bytes} = read_data(This, 1),
  case Bytes of
    {ok, <<Val:8/integer-signed-big>>} ->
      {This1, {ok, Val}};
    Err ->
      {This1, Err}
  end;

read(This, i16) ->
  {This1, {ok, I16}} = read_varint(This),
  {This1, {ok, zigzag_to_number(I16)}};

read(This, i32) ->
  {This1, {ok, I32}} = read_varint(This),
  {This1, {ok, zigzag_to_number(I32)}};

read(This, i64) ->
  {This1, {ok, I64}} = read_varint(This),
  {This1, {ok, zigzag_to_number(I64)}};

read(This, varint) ->
  %lager:debug("read_varint"),
  read_varint(This);

read(This, double) ->
  {This1, Initial} = read_data(This, 8),
  case Initial of
    {ok, Bin} ->
      <<Double:64/little-signed-float>> = Bin,
      {This1, {ok, Double}};
    Other ->
      {This1, Other}
  end;

read(This, void) ->
  %lager:warning("read_void ????"),
  {This, ok};

read(Transport, Data) ->
  %lager:warning("read_data: Data=~p", [Data]),
  {Transport, Data}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Factory generation
%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(tcp_opts, {
  strict_read,
  strict_write
}).

% Possible options
% {strict_read, boolean()}
% {strict_write, boolean()}
parse_factory_options([], Opts) ->
  Opts;
parse_factory_options([{strict_read, Bool} | Rest], Opts) ->
  parse_factory_options(Rest, Opts#tcp_opts{strict_read = Bool});
parse_factory_options([{strict_write, Bool} | Rest], Opts) ->
  parse_factory_options(Rest, Opts#tcp_opts{strict_write = Bool}).

% create the factory func
new_protocol_factory(TransportFactory, Options) ->
  ParsedOpts = parse_factory_options(Options, #tcp_opts{}),
  F =
    fun() ->
      {ok, Transport} = TransportFactory(),
      thrift_compact_protocol:new(
        Transport,
        [{strict_read, ParsedOpts#tcp_opts.strict_read},
         {strict_write, ParsedOpts#tcp_opts.strict_write}])
    end,
  {ok, F}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Helpers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%



read_data(This, 0) ->
  {This, {ok, <<>>}};
read_data(This = #compact_protocol{transport = Trans}, Len) when is_integer(Len) andalso Len > 0 ->
  {NewTransport, Result} = thrift_transport:read(Trans, Len),
  {This#compact_protocol{transport = NewTransport}, Result}.

parse_options(Opts, State) ->
  SR = proplists:get_value(strict_read, Opts, false),
  SW = proplists:get_value(strict_write, Opts, false),
  State#compact_protocol{strict_read = SR, strict_write = SW}.

number_to_zigzag(Num, i16) -> number_to_zigzag(Num, i32);
number_to_zigzag(Num, i32) -> (Num bsl 1) bxor (Num bsr 31);
number_to_zigzag(Num, i64) -> (Num bsl 1) bxor (Num bsr 63).

zigzag_to_number(Num) -> (Num bsr 1) bxor (-(Num band 1)).

encode_varint(I) ->
  R = encode_varint(I, []),
  %lager:debug("encode_varint: ~p -> ~p", [I, R]),
  R.

encode_varint(I, Acc) when I =< 16#7f ->
  lists:reverse([I | Acc]);
encode_varint(I, Acc) ->
  %lager:debug("encode_varint: ~p, ~p", [I, Acc]),
  Last_Seven_Bits = (I - ((I bsr 7) bsl 7)),
  First_X_Bits = (I bsr 7),
  With_Leading_Bit = Last_Seven_Bits bor 16#80,
  encode_varint(First_X_Bits, [With_Leading_Bit|Acc]).


read_varint(This) ->
  read_varint(This, []).

read_varint(This, Acc) ->
  {This1, {ok, Byte}} = read(This, byte),
  case Byte band 16#80 of
    16#80 ->
      Last_Seven_Bits = (Byte - ((Byte bsr 7) bsl 7)),
      read_varint(This1, [Last_Seven_Bits|Acc]);
    _ ->
      L = lists:reverse([Byte|Acc]),
      {_, Num} = lists:foldl(fun(I, {S, A}) -> {S+7, A bor (I bsl S)} end, {0, 0}, L),
      %lager:debug("read_varint: Num: ~p, L: ~p", [Num, L]),
      {This1, {ok, Num}}
  end.

read_binary(This, 0) ->
  %lager:debug("read_binary: ~p", [0]),
  {This, {ok, <<"">>}};
read_binary(This, Length) ->
  %lager:debug("read_binary: ~p", [Length]),
  {This1, Read} = read_data(This, Length),
  case Read of
    {ok, Binary} ->
      {This1, {ok, Binary}};
    Other ->
      {This1, Other}
  end.

write_field_begin_internal(This=#compact_protocol{
      last_field_id = LastFieldId
    },
    _Field=#protocol_field_begin{type = FType, id = FId},
    TypeOverride) ->
  ActualType = case TypeOverride == -1 of
    true -> get_compact_type(FType);
    false -> TypeOverride
  end,
  {This1, Ret} = case FId > LastFieldId andalso (FId - LastFieldId) =< 15 of
    true ->
      write(This, (FId - LastFieldId) bsl 4 bor ActualType);
    false ->
      write_many(This, [{byte, ActualType}, {i16, FId}])
  end,
  {This1#compact_protocol{
    last_field_id = FId
  }, Ret}.

write_many(This, Fields) ->
  Fun =
    fun
      (F, {T, ok}) ->
        write(T, F);
      (_F, Err) -> Err
    end,
  lists:foldl(Fun, {This, ok}, Fields).

is_bool_type(Type) ->
  %lager:debug("is_bool_type: Type=~p", [Type]),
  LowerNibble = Type band 16#0f,
  case LowerNibble == ?Types_BOOLEAN_TRUE orelse LowerNibble == ?Types_BOOLEAN_FALSE of
    true ->
      %lager:debug("is_bool_type: true"),
      true;
    _ ->
      %lager:debug("is_bool_type: false"),
      false
  end.

get_ubyte(SByte) ->
  Bin = <<SByte:8/integer-signed-big>>,
  <<UByte:8/integer-unsigned-big>> = Bin,
  UByte.

get_sbyte(UByte) ->
  Bin = <<UByte:8/integer-unsigned-big>>,
  <<SByte:8/integer-signed-big>> = Bin,
  SByte.

get_ttype(Type) ->
  get_thrift_type(Type band 16#0f).

get_thrift_type(?tType_STOP) -> ?tType_STOP;
get_thrift_type(?Types_BOOLEAN_TRUE) -> ?tType_BOOL;
get_thrift_type(?Types_BOOLEAN_FALSE) -> ?tType_BOOL;
get_thrift_type(?Types_BYTE) -> ?tType_BYTE;
get_thrift_type(?Types_I16) -> ?tType_I16;
get_thrift_type(?Types_I32) -> ?tType_I32;
get_thrift_type(?Types_I64) -> ?tType_I64;
get_thrift_type(?Types_DOUBLE) -> ?tType_DOUBLE;
get_thrift_type(?Types_BINARY) -> ?tType_STRING;
get_thrift_type(?Types_LIST) -> ?tType_LIST;
get_thrift_type(?Types_SET) -> ?tType_SET;
get_thrift_type(?Types_MAP) -> ?tType_MAP;
get_thrift_type(?Types_STRUCT) -> ?tType_STRUCT.

get_compact_type(?tType_STOP) -> ?tType_STOP;
get_compact_type(?tType_BOOL) -> ?Types_BOOLEAN_TRUE;
get_compact_type(?tType_BYTE) -> ?Types_BYTE;
get_compact_type(?tType_I16) -> ?Types_I16;
get_compact_type(?tType_I32) -> ?Types_I32;
get_compact_type(?tType_I64) -> ?Types_I64;
get_compact_type(?tType_DOUBLE) -> ?Types_DOUBLE;
get_compact_type(?tType_STRING) -> ?Types_BINARY;
get_compact_type(?tType_LIST) -> ?Types_LIST;
get_compact_type(?tType_SET) -> ?Types_SET;
get_compact_type(?tType_MAP) -> ?Types_MAP;
get_compact_type(?tType_STRUCT) -> ?Types_STRUCT.

write_collection_begin(This, ElemType, Size) when Size =< 14 ->
  write(This, {byte, (Size bsl 4) bor get_compact_type(ElemType)});
write_collection_begin(This, ElemType, Size) ->
  write_many(This, [{byte, 16#f bor get_compact_type(ElemType)}, {i32, Size}]).

stack_push(E, Stack) ->
  [E] ++ Stack.

stack_pop([]) ->
  [];
stack_pop([H|Rest]) ->
  {H, Rest}.
