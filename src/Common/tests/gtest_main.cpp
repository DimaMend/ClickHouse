#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>

#include <Common/QuillLogger.h>
#include <Common/Logger.h>

class ContextEnvironment : public testing::Environment
{
public:
    void SetUp() override
    {
        DB::startQuillBackend();
        getContext();
    }
    void TearDown() override { disableLogging(); }
};

int main(int argc, char ** argv)
{
    testing::InitGoogleTest(&argc, argv);

    testing::AddGlobalTestEnvironment(new ContextEnvironment);

    return RUN_ALL_TESTS();
}
