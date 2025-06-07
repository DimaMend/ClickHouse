#include "Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h"
#include "config.h"

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteTransform.h>

#    include <Core/Settings.h>
#    include <Formats/FormatFactory.h>
#    include <Formats/ReadSchemaUtils.h>
#    include <IO/CompressionMethod.h>
#    include <IO/ReadBufferFromFileBase.h>
#    include <Interpreters/Context.h>
#    include <Interpreters/ExpressionAnalyzer.h>
#    include <Parsers/ASTFunction.h>
#    include <Parsers/ASTIdentifier.h>
#    include <Parsers/ASTLiteral.h>
#    include <Processors/Formats/ISchemaReader.h>
#    include <Storages/ObjectStorage/StorageObjectStorageSource.h>

namespace DB
{

namespace Setting
{
extern const SettingsNonZeroUInt64 max_block_size;
}

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

void IcebergPositionDeleteTransform::initializeDeleteSources()
{
    /// Create filter on the data object to get interested rows
    auto iceberg_data_path = iceberg_object_info->getIcebergDataPath();
    ASTPtr where_ast = makeASTFunction(
        "equals",
        std::make_shared<ASTIdentifier>(IcebergPositionDeleteTransform::filename_column_name),
        std::make_shared<ASTLiteral>(Field(iceberg_data_path)));

    for (const auto & position_deletes_object : iceberg_object_info->position_deletes_objects)
    {
        Iceberg::PositionalDeleteFileSpecificInfo specific_info
            = std::get<Iceberg::PositionalDeleteFileSpecificInfo>(position_deletes_object.specific_info);
        if (specific_info.reference_file_path.has_value() && specific_info.reference_file_path != iceberg_data_path)
            continue; // Skip position deletes that do not match the data file path.

        auto object_path = position_deletes_object.file_name;
        auto object_metadata = object_storage->getObjectMetadata(object_path);
        auto object_info = std::make_shared<ObjectInfo>(object_path, object_metadata);


        Block initial_header;
        {
            std::unique_ptr<ReadBuffer> read_buf_schema
                = StorageObjectStorageSource::createReadBuffer(*object_info, object_storage, context, log);
            auto schema_reader = FormatFactory::instance().getSchemaReader(delete_object_format, *read_buf_schema, context);
            auto columns_with_names = schema_reader->readSchema();
            ColumnsWithTypeAndName initial_header_data;
            for (const auto & elem : columns_with_names)
            {
                initial_header_data.push_back(ColumnWithTypeAndName(elem.type, elem.name));
            }
            initial_header = Block(initial_header_data);
        }

        CompressionMethod compression_method = chooseCompressionMethod(object_path, delete_object_compression_method);

        delete_read_buffers.push_back(StorageObjectStorageSource::createReadBuffer(*object_info, object_storage, context, log));

        auto delete_format = FormatFactory::instance().getInput(
            delete_object_format,
            *delete_read_buffers.back(),
            initial_header,
            context,
            context->getSettingsRef()[DB::Setting::max_block_size],
            format_settings,
            1,
            std::nullopt,
            true /* is_remote_fs */,
            compression_method);

        auto syntax_result = TreeRewriter(context).analyze(where_ast, initial_header.getNamesAndTypesList());
        ExpressionAnalyzer analyzer(where_ast, syntax_result, context);
        const std::optional<ActionsDAG> actions = analyzer.getActionsDAG(true);
        delete_format->setKeyCondition(actions, context);

        delete_sources.push_back(std::move(delete_format));
    }
}

size_t IcebergPositionDeleteTransform::getColumnIndex(const std::shared_ptr<IInputFormat> & delete_source, const String & column_name)
{
    const auto & delete_header = delete_source->getOutputs().back().getHeader();
    for (size_t i = 0; i < delete_header.getNames().size(); ++i)
    {
        if (delete_header.getNames()[i] == column_name)
        {
            return i;
        }
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not find column {} in chunk", column_name);
}

void IcebergBitmapPositionDeleteTransform::transform(Chunk & chunk)
{
    size_t num_rows = chunk.getNumRows();
    IColumn::Filter delete_vector(num_rows, true);
    size_t num_rows_after_filtration = num_rows;

    auto chunk_info = chunk.getChunkInfos().get<ChunkInfoRowNumOffset>();
    if (!chunk_info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ChunkInfoRowNumOffset does not exist");

    size_t row_num_offset = chunk_info->row_num_offset;
    for (size_t i = 0; i < num_rows; i++)
    {
        size_t row_idx = row_num_offset + i;
        if (bitmap.rb_contains(row_idx))
        {
            delete_vector[i] = false;
            num_rows_after_filtration--;
        }
    }

    auto columns = chunk.detachColumns();
    for (auto & column : columns)
        column = column->filter(delete_vector, -1);

    chunk.setColumns(std::move(columns), num_rows_after_filtration);
}

void IcebergBitmapPositionDeleteTransform::initialize()
{
    auto iceberg_data_path = iceberg_object_info->getIcebergDataPath();
    for (auto & delete_source : delete_sources)
    {
        while (auto delete_chunk = delete_source->read())
        {
            int position_index = getColumnIndex(delete_source, IcebergPositionDeleteTransform::positions_column_name);
            int filename_index = getColumnIndex(delete_source, IcebergPositionDeleteTransform::filename_column_name);

            auto position_column = delete_chunk.getColumns()[position_index];
            auto filename_column = delete_chunk.getColumns()[filename_index];

            for (size_t i = 0; i < delete_chunk.getNumRows(); ++i)
            {
                auto position_to_delete = position_column->get64(i);
                auto filename_to_delete = filename_column->getDataAt(i).toString();
                if (filename_to_delete == iceberg_data_path)
                {
                    bitmap.add(position_to_delete);
                }
            }
        }
    }
}

}

#endif
