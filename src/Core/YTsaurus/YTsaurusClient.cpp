#include "config.h"

#if USE_YTSAURUS

#include "YTsaurusClient.h"

#include <IO/HTTPHeaderEntries.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromOStream.h>
#include <Formats/formatBlock.h>
#include <Core/Block.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/JSON/Parser.h>

#include <Interpreters/Context_fwd.h>
#include <QueryPipeline/Pipe.h>


#include <memory>


namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int ALL_CONNECTION_TRIES_FAILED;
}

namespace Setting
{
    extern const SettingsUInt64 max_http_get_redirects;
}

YTsaurusClient::YTsaurusClient(ContextPtr context_, const ConnectionInfo & connection_info_)
    : context(context_), connection_info(connection_info_), log(getLogger("YTsaurusClient"))
{
}


DB::ReadBufferPtr YTsaurusClient::readTable(const String & cypress_path)
{
    YTsaurusQueryPtr read_table_query(new YTsaurusReadTableQuery(cypress_path));
    return createQueryRWBuffer(read_table_query);
}

YTsaurusNodeType YTsaurusClient::getNodeType(const String & cypress_path)
{
    String attributes_path = cypress_path + "/@";
    YTsaurusQueryPtr get_query(new YTsaurusGetQuery(attributes_path));
    auto buf = createQueryRWBuffer(get_query);

    String json_str;
    readJSONObjectPossiblyInvalid(json_str, *buf);

    Poco::JSON::Parser parser;
    Poco::Dynamic::Var json = parser.parse(json_str);
    const Poco::JSON::Object::Ptr & json_ptr = json.extract<Poco::JSON::Object::Ptr>();
    return getNodeTypeFromAttributes(json_ptr);
}


YTsaurusNodeType YTsaurusClient::getNodeTypeFromAttributes(const Poco::JSON::Object::Ptr json_ptr)
{
    if (!json_ptr->has("type"))
        throw DB::Exception(DB::ErrorCodes::INCORRECT_DATA, "Incorrect json with yt attributes, no field 'type'.");

    if (json_ptr->getValue<String>("type") == "table")
    {
        if (!json_ptr->has("dynamic"))
            throw DB::Exception(DB::ErrorCodes::INCORRECT_DATA, "Incorrect json with yt attributes, no field 'dynamic'.");

        return json_ptr->getValue<bool>("dynamic") ? YTsaurusNodeType::DYNAMIC_TABLE : YTsaurusNodeType::STATIC_TABLE;
    }
    else
    {
        return YTsaurusNodeType::ANOTHER;
    }
}

DB::ReadBufferPtr YTsaurusClient::selectRows(const String & cypress_path)
{
    YTsaurusQueryPtr select_rows_query(new YTsaurusSelectRowsQuery(cypress_path));
    return createQueryRWBuffer(select_rows_query);
}

DB::ReadBufferPtr YTsaurusClient::lookupRows(const String & cypress_path, const Block & lookup_block_input)
{
    YTsaurusQueryPtr lookup_rows_query(new YTsaurusLookupRows(cypress_path));
    auto out_callback = [lookup_block_input, this](std::ostream & ostr)
    {
        FormatSettings format_settings;
        format_settings.json.quote_64bit_integers = false;
        WriteBufferFromOStream out_buffer(ostr);
        auto output_format = context->getOutputFormat("JSONEachRow", out_buffer, lookup_block_input.cloneEmpty(), format_settings);
        formatBlock(output_format, lookup_block_input);
        out_buffer.finalize();
    };
    return createQueryRWBuffer(lookup_rows_query, std::move(out_callback));
}


DB::ReadBufferPtr YTsaurusClient::createQueryRWBuffer(const YTsaurusQueryPtr query, ReadWriteBufferFromHTTP::OutStreamCallback out_callback)
{
    for (size_t num_try = 0; num_try < connection_info.http_proxy_urls.size(); ++num_try)
    {
        size_t url_index = (recently_used_url_index + num_try) % connection_info.http_proxy_urls.size();
        try
        {
            Poco::URI uri(connection_info.http_proxy_urls[url_index].c_str());
            uri.setPath(fmt::format("/api/{}/{}", connection_info.api_version, query->getQueryName()));

            for (const auto & query_param : query->getQueryParameters())
            {
                uri.addQueryParameter(query_param.name, query_param.value);
            }

            DB::HTTPHeaderEntries http_headers{
                /// Always use json format for input and output.
                {"Accept", "application/json"},
                {"Content-Type", "application/json"},
                {"Authorization", fmt::format("OAuth {}", connection_info.oauth_token)},
            };

            LOG_TRACE(log, "URI {} , query type {}", uri.toString(), query->getQueryName());
            Poco::Net::HTTPBasicCredentials creds;
            auto buf = DB::BuilderRWBufferFromHTTP(uri)
                        .withConnectionGroup(DB::HTTPConnectionGroupType::STORAGE)
                        .withMethod(query->getHTTPMethod())
                        .withSettings(context->getReadSettings())
                        .withTimeouts(DB::ConnectionTimeouts::getHTTPTimeouts(context->getSettingsRef(), context->getServerSettings()))
                        .withHostFilter(&context->getRemoteHostFilter())
                        .withRedirects(context->getSettingsRef()[Setting::max_http_get_redirects])
                        .withOutCallback(out_callback)
                        .withHeaders(http_headers)
                        .withDelayInit(false)
                        .create(creds);

            recently_used_url_index = url_index;
            return DB::ReadBufferPtr(std::move(buf));
        }
        catch (Exception & e)
        {
            LOG_WARNING(log, "Error while creating connection with {}, will try to use another http proxy if there are any. Exception: {}",
                connection_info.http_proxy_urls[url_index], e.displayText());
        }
    }
    throw Exception(ErrorCodes::ALL_CONNECTION_TRIES_FAILED, "All connection tries with ytsaurus http proxies are failed.");
}

}
#endif
