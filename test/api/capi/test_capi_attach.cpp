#include "catch.hpp"
#include "duckdb.h"
#include "test_helpers.hpp"

using namespace duckdb;

namespace {

struct AttachState {
        bool attach_called = false;
        bool transaction_called = false;
};

static void StorageAttachCallback(duckdb_attach_info info) {
        auto *state = static_cast<AttachState *>(duckdb_attach_info_get_extra_info(info));
        REQUIRE(state);
        state->attach_called = true;

        REQUIRE(std::string(duckdb_attach_info_get_name(info)) == "capi_ext");

        const char *path = duckdb_attach_info_get_path(info);
        REQUIRE(path);
        duckdb_attach_info_set_path(info, path);

        duckdb_client_context context = nullptr;
        duckdb_attach_info_get_client_context(info, &context);
        REQUIRE(context != nullptr);
        duckdb_destroy_client_context(&context);

        const char *custom_value = nullptr;
        REQUIRE(duckdb_attach_info_get_option_varchar(info, "custom", &custom_value));
        REQUIRE(std::string(custom_value) == "value");

        bool flag = false;
        REQUIRE(duckdb_attach_info_get_option_boolean(info, "flag", &flag));
        REQUIRE(flag);

        int64_t count = 0;
        REQUIRE(duckdb_attach_info_get_option_bigint(info, "count", &count));
        REQUIRE(count == 42);

        double ratio = 0;
        REQUIRE(duckdb_attach_info_get_option_double(info, "ratio", &ratio));
        REQUIRE(ratio == Approx(1.5));

        duckdb_attach_info_set_duck_catalog(info);
}

static void StorageTransactionCallback(duckdb_transaction_info info) {
        auto *state = static_cast<AttachState *>(duckdb_transaction_info_get_extra_info(info));
        REQUIRE(state);
        state->transaction_called = true;
        duckdb_transaction_info_set_duck_transaction_manager(info);
}

static void FailingAttachCallback(duckdb_attach_info info) {
        duckdb_attach_info_set_error(info, "attach failure");
}

} // namespace

TEST_CASE("C API storage extension attach", "[capi]") {
        duckdb_storage_extension extension = duckdb_create_storage_extension();
        AttachState state;
        duckdb_storage_extension_set_extra_info(extension, &state, nullptr);
        duckdb_storage_extension_set_attach(extension, StorageAttachCallback);
        duckdb_storage_extension_set_transaction(extension, StorageTransactionCallback);

        duckdb_config config;
        REQUIRE(duckdb_create_config(&config) == DuckDBSuccess);
        REQUIRE(duckdb_config_add_storage_extension(config, "cstorage", extension) == DuckDBSuccess);

        duckdb_database db;
        REQUIRE(duckdb_open_ext(nullptr, &db, config, nullptr) == DuckDBSuccess);
        duckdb_connection conn;
        REQUIRE(duckdb_connect(db, &conn) == DuckDBSuccess);

        duckdb_result result;
        REQUIRE(duckdb_query(conn,
                              "ATTACH ':memory:' AS capi_ext (TYPE cstorage, custom 'value', flag true, count 42, ratio 1.5)",&
                              result) == DuckDBSuccess);
        duckdb_destroy_result(&result);

        REQUIRE(state.attach_called);
        REQUIRE(state.transaction_called);

        REQUIRE(duckdb_query(conn, "CREATE TABLE capi_ext.main.attached_tbl AS SELECT 42 AS value", &result) ==
                DuckDBSuccess);
        duckdb_destroy_result(&result);

        REQUIRE(duckdb_query(conn, "SELECT value FROM capi_ext.main.attached_tbl", &result) == DuckDBSuccess);
        REQUIRE(result.column_count == 1);
        REQUIRE(result.row_count == 1);
        REQUIRE(duckdb_value_int64(&result, 0, 0) == 42);
        duckdb_destroy_result(&result);

        REQUIRE(duckdb_query(conn, "DETACH capi_ext", &result) == DuckDBSuccess);
        duckdb_destroy_result(&result);

        duckdb_disconnect(&conn);
        duckdb_close(&db);
        duckdb_destroy_config(&config);
}

TEST_CASE("C API storage extension attach failure", "[capi]") {
        duckdb_storage_extension extension = duckdb_create_storage_extension();
        duckdb_storage_extension_set_attach(extension, FailingAttachCallback);
        duckdb_storage_extension_set_transaction(extension, StorageTransactionCallback);

        duckdb_config config;
        REQUIRE(duckdb_create_config(&config) == DuckDBSuccess);
        REQUIRE(duckdb_config_add_storage_extension(config, "cfail", extension) == DuckDBSuccess);

        duckdb_database db;
        REQUIRE(duckdb_open_ext(nullptr, &db, config, nullptr) == DuckDBSuccess);
        duckdb_connection conn;
        REQUIRE(duckdb_connect(db, &conn) == DuckDBSuccess);

        duckdb_result result;
        REQUIRE(duckdb_query(conn, "ATTACH ':memory:' AS failing (TYPE cfail)", &result) == DuckDBError);
        duckdb_destroy_result(&result);

        duckdb_disconnect(&conn);
        duckdb_close(&db);
        duckdb_destroy_config(&config);
}
