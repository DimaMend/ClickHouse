#include "config.h"

#if USE_YTSAURUS
#include "YTsaurusSource.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

YTsaurusTableSourceStaticTable::YTsaurusTableSourceStaticTable(
    YTsaurusClientPtr client_, const String & cypress_path, const Block & sample_block_, const UInt64 & max_block_size_, const bool skip_unknown_columns)
    : ISource(sample_block_), client(std::move(client_)), sample_block(sample_block_), max_block_size(max_block_size_)
{
    read_buffer = client->readTable(cypress_path);
    FormatSettings format_settings{.skip_unknown_fields = skip_unknown_columns};

    json_row_format = std::make_unique<JSONEachRowRowInputFormat>(
        *read_buffer.get(), sample_block, IRowInputFormat::Params({.max_block_size = max_block_size}), format_settings, false);
}


YTsaurusTableSourceDynamicTable::YTsaurusTableSourceDynamicTable(
    YTsaurusClientPtr client_, const YTsaurusTableSourceOptions & source_options_, const Block & sample_block_, const UInt64 & max_block_size_)
    : ISource(sample_block_)
    , client(std::move(client_))
    , source_options(source_options_)
    , sample_block(sample_block_)
    , max_block_size(max_block_size_)
    , format_settings({.skip_unknown_fields = source_options.skip_unknown_columns})
    , use_lookups(!source_options.force_read_table && source_options.lookup_input_block)
{
    read_buffer = (use_lookups) ? client->lookupRows(source_options.cypress_path, *source_options.lookup_input_block) : client->selectRows(source_options.cypress_path);

    json_row_format = std::make_unique<JSONEachRowRowInputFormat>(
        *read_buffer.get(), sample_block, IRowInputFormat::Params({.max_block_size = max_block_size}), format_settings, false);
}

std::shared_ptr<ISource> YTsaurusSourceFactory::createSource(YTsaurusClientPtr client, const YTsaurusTableSourceOptions source_options, const Block & sample_block, const UInt64 max_block_size)
{
    if (source_options.cypress_path.empty())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cypress path are empty for ytsarurus source factory.");
    }
    auto yt_node_type = client->getNodeType(source_options.cypress_path);
    if (yt_node_type == YTsaurusNodeType::STATIC_TABLE)
    {
        return std::make_shared<YTsaurusTableSourceStaticTable>(client, source_options.cypress_path, sample_block, max_block_size);
    }
    else if (yt_node_type == YTsaurusNodeType::DYNAMIC_TABLE)
    {
        return std::make_shared<YTsaurusTableSourceDynamicTable>(client, source_options, sample_block, max_block_size);
    }
    else
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Node {} has unsupported type.", source_options.cypress_path);
    }
}

}
#endif
