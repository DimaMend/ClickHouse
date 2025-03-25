#pragma once

#include "Common/DateLUTImpl.h"
#include "DataTypes/DataTypeDate.h"
#include "DataTypes/DataTypeDate32.h"
#include "DataTypes/DataTypeDateTime.h"
#include "DataTypes/DataTypeIPv4andIPv6.h"
#include "DataTypes/DataTypeUUID.h"
#include "IO/Operators.h"
#include "IO/ReadHelpers.h"
#include "IO/WriteBufferFromString.h"
#include "base/extended_types.h"
#include <IO/ReadBuffer.h>
#include <Functions/ConvertImpl.h>

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_PARSE_TEXT;
}

namespace detail
{
/** Conversion of strings to numbers, dates, datetimes: through parsing.
  */
template <typename DataType>
void parseImpl(typename DataType::FieldType & x, ReadBuffer & rb, const DateLUTImpl *, bool precise_float_parsing)
{
    if constexpr (is_floating_point<typename DataType::FieldType>)
    {
        if (precise_float_parsing)
            readFloatTextPrecise(x, rb);
        else
            readFloatTextFast(x, rb);
    }
    else
        readText(x, rb);
}

template <>
inline void parseImpl<DataTypeDate>(DataTypeDate::FieldType & x, ReadBuffer & rb, const DateLUTImpl * time_zone, bool)
{
    DayNum tmp(0);
    readDateText(tmp, rb, *time_zone);
    x = tmp;
}

template <>
inline void parseImpl<DataTypeDate32>(DataTypeDate32::FieldType & x, ReadBuffer & rb, const DateLUTImpl * time_zone, bool)
{
    ExtendedDayNum tmp(0);
    readDateText(tmp, rb, *time_zone);
    x = tmp;
}


// NOTE: no need of extra overload of DateTime64, since readDateTimeText64 has different signature and that case is explicitly handled in the calling code.
template <>
inline void parseImpl<DataTypeDateTime>(DataTypeDateTime::FieldType & x, ReadBuffer & rb, const DateLUTImpl * time_zone, bool)
{
    time_t time = 0;
    readDateTimeText(time, rb, *time_zone);
    convertFromTime<DataTypeDateTime>(x, time);
}

template <>
inline void parseImpl<DataTypeUUID>(DataTypeUUID::FieldType & x, ReadBuffer & rb, const DateLUTImpl *, bool)
{
    UUID tmp;
    readUUIDText(tmp, rb);
    x = tmp.toUnderType();
}

template <>
inline void parseImpl<DataTypeIPv4>(DataTypeIPv4::FieldType & x, ReadBuffer & rb, const DateLUTImpl *, bool)
{
    IPv4 tmp;
    readIPv4Text(tmp, rb);
    x = tmp.toUnderType();
}

template <>
inline void parseImpl<DataTypeIPv6>(DataTypeIPv6::FieldType & x, ReadBuffer & rb, const DateLUTImpl *, bool)
{
    IPv6 tmp;
    readIPv6Text(tmp, rb);
    x = tmp;
}

template <typename DataType>
bool tryParseImpl(typename DataType::FieldType & x, ReadBuffer & rb, const DateLUTImpl *, bool precise_float_parsing)
{
    if constexpr (is_floating_point<typename DataType::FieldType>)
    {
        if (precise_float_parsing)
            return tryReadFloatTextPrecise(x, rb);
        else
            return tryReadFloatTextFast(x, rb);
    }
    else /*if constexpr (is_integral_v<typename DataType::FieldType>)*/
        return tryReadIntText(x, rb);
}

  template <>
  inline bool tryParseImpl<DataTypeDate>(DataTypeDate::FieldType & x, ReadBuffer & rb, const DateLUTImpl * time_zone, bool)
  {
      DayNum tmp(0);
      if (!tryReadDateText(tmp, rb, *time_zone))
          return false;
      x = tmp;
      return true;
  }

  template <>
  inline bool tryParseImpl<DataTypeDate32>(DataTypeDate32::FieldType & x, ReadBuffer & rb, const DateLUTImpl * time_zone, bool)
  {
      ExtendedDayNum tmp(0);
      if (!tryReadDateText(tmp, rb, *time_zone))
          return false;
      x = tmp;
      return true;
  }

  template <>
  inline bool tryParseImpl<DataTypeDateTime>(DataTypeDateTime::FieldType & x, ReadBuffer & rb, const DateLUTImpl * time_zone, bool)
  {
      time_t time = 0;
      if (!tryReadDateTimeText(time, rb, *time_zone))
          return false;
      convertFromTime<DataTypeDateTime>(x, time);
      return true;
  }

  template <>
  inline bool tryParseImpl<DataTypeUUID>(DataTypeUUID::FieldType & x, ReadBuffer & rb, const DateLUTImpl *, bool)
  {
      UUID tmp;
      if (!tryReadUUIDText(tmp, rb))
          return false;

      x = tmp.toUnderType();
      return true;
  }

  template <>
  inline bool tryParseImpl<DataTypeIPv4>(DataTypeIPv4::FieldType & x, ReadBuffer & rb, const DateLUTImpl *, bool)
  {
      IPv4 tmp;
      if (!tryReadIPv4Text(tmp, rb))
          return false;

      x = tmp.toUnderType();
      return true;
  }

  template <>
  inline bool tryParseImpl<DataTypeIPv6>(DataTypeIPv6::FieldType & x, ReadBuffer & rb, const DateLUTImpl *, bool)
  {
      IPv6 tmp;
      if (!tryReadIPv6Text(tmp, rb))
          return false;

      x = tmp;
      return true;
  }


  /** Throw exception with verbose message when string value is not parsed completely.
    */
  [[noreturn]] inline void throwExceptionForIncompletelyParsedValue(ReadBuffer & read_buffer, const IDataType & result_type)
  {
      WriteBufferFromOwnString message_buf;
      message_buf << "Cannot parse string " << quote << String(read_buffer.buffer().begin(), read_buffer.buffer().size())
                  << " as " << result_type.getName()
                  << ": syntax error";

      if (read_buffer.offset())
          message_buf << " at position " << read_buffer.offset()
                      << " (parsed just " << quote << String(read_buffer.buffer().begin(), read_buffer.offset()) << ")";
      else
          message_buf << " at begin of string";

      // Currently there are no functions toIPv{4,6}Or{Null,Zero}
      if (isNativeNumber(result_type) && !(result_type.getName() == "IPv4" || result_type.getName() == "IPv6"))
          message_buf << ". Note: there are to" << result_type.getName() << "OrZero and to" << result_type.getName() << "OrNull functions, which returns zero/NULL instead of throwing exception.";

      throw Exception(PreformattedMessage{message_buf.str(), "Cannot parse string {} as {}: syntax error {}", {String(read_buffer.buffer().begin(), read_buffer.buffer().size()), result_type.getName()}}, ErrorCodes::CANNOT_PARSE_TEXT);
  }
  }
}
