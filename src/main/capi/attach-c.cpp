#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"

namespace duckdb {

struct CStorageExtension;

struct CAttachFunctionInfo {
        CAttachFunctionInfo(optional_ptr<StorageExtensionInfo> storage_info_p, ClientContext &context_p,
                            AttachedDatabase &db_p, const string &name_p, AttachInfo &attach_info_p,
                            AttachOptions &options_p, CStorageExtension &extension_p)
            : storage_info(storage_info_p), context(context_p), db(db_p), name(name_p), attach_info(attach_info_p),
              options(options_p), extension(extension_p) {
        }

        optional_ptr<StorageExtensionInfo> storage_info;
        ClientContext &context;
        AttachedDatabase &db;
        const string &name;
        AttachInfo &attach_info;
        AttachOptions &options;
        CStorageExtension &extension;

        unique_ptr<Catalog> catalog;
        string error;
        bool success = true;
        vector<string> string_cache;

        Value *GetOptionValue(const string &key) {
                auto entry = options.options.find(key);
                if (entry != options.options.end()) {
                        return &entry->second;
                }
                auto info_entry = attach_info.options.find(key);
                if (info_entry != attach_info.options.end()) {
                        return &info_entry->second;
                }
                return nullptr;
        }
};

struct CTransactionInfo {
        CTransactionInfo(optional_ptr<StorageExtensionInfo> storage_info_p, AttachedDatabase &db_p, Catalog &catalog_p,
                         CStorageExtension &extension_p)
            : storage_info(storage_info_p), db(db_p), catalog(catalog_p), extension(extension_p) {
        }

        optional_ptr<StorageExtensionInfo> storage_info;
        AttachedDatabase &db;
        Catalog &catalog;
        CStorageExtension &extension;

        unique_ptr<TransactionManager> transaction_manager;
        string error;
        bool success = true;
};

struct CAPIStorageExtensionInfo : public StorageExtensionInfo {
        explicit CAPIStorageExtensionInfo(CStorageExtension &extension_p) : extension(extension_p) {
        }

        CStorageExtension &extension;
};

struct CStorageExtension : public StorageExtension {
        CStorageExtension() {
                auto info = make_shared_ptr<CAPIStorageExtensionInfo>(*this);
                storage_info = info;
                attach = CStorageExtension::AttachCallback;
                create_transaction_manager = CStorageExtension::CreateTransactionManagerCallback;
        }

        ~CStorageExtension() override {
                if (extra_info && extra_info_delete) {
                        extra_info_delete(extra_info);
                }
        }

        static CStorageExtension &GetExtension(optional_ptr<StorageExtensionInfo> storage_info) {
                if (!storage_info) {
                        throw InternalException("Storage extension info was not provided to attach callback");
                }
                auto &info = *static_cast<CAPIStorageExtensionInfo *>(storage_info.get());
                return info.extension;
        }

        static unique_ptr<Catalog> AttachCallback(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                                  AttachedDatabase &db, const string &name, AttachInfo &attach_info,
                                                  AttachOptions &options) {
                auto &extension = GetExtension(storage_info);
                if (!extension.attach_callback) {
                        throw InvalidInputException("Attach callback was not set for storage extension");
                }

                CAttachFunctionInfo info(storage_info, context, db, name, attach_info, options, extension);
                extension.attach_callback(reinterpret_cast<duckdb_attach_info>(&info));
                if (!info.success) {
                        throw InvalidInputException(info.error);
                }
                if (!info.catalog) {
                        throw InvalidInputException("Attach callback did not produce a catalog");
                }
                return std::move(info.catalog);
        }

        static unique_ptr<TransactionManager> CreateTransactionManagerCallback(
            optional_ptr<StorageExtensionInfo> storage_info, AttachedDatabase &db, Catalog &catalog) {
                auto &extension = GetExtension(storage_info);
                if (!extension.create_transaction_manager_callback) {
                        throw InvalidInputException("Create transaction manager callback was not set for storage extension");
                }

                CTransactionInfo info(storage_info, db, catalog, extension);
                extension.create_transaction_manager_callback(reinterpret_cast<duckdb_transaction_info>(&info));
                if (!info.success) {
                        throw InvalidInputException(info.error);
                }
                if (!info.transaction_manager) {
                        throw InvalidInputException("Create transaction manager callback did not produce a transaction manager");
                }
                return std::move(info.transaction_manager);
        }

        void SetExtraInfo(void *extra_info_p, duckdb_delete_callback_t delete_callback) {
                if (extra_info && extra_info_delete) {
                        extra_info_delete(extra_info);
                }
                extra_info = extra_info_p;
                extra_info_delete = delete_callback;
        }

        void *GetExtraInfo() {
                return extra_info;
        }

        ::duckdb_storage_attach_t attach_callback = nullptr;
        ::duckdb_storage_create_transaction_t create_transaction_manager_callback = nullptr;

private:
        void *extra_info = nullptr;
        duckdb_delete_callback_t extra_info_delete = nullptr;
};

} // namespace duckdb

using duckdb::CStorageExtension;

struct CStorageExtensionWrapper {
        duckdb::unique_ptr<CStorageExtension> extension;
};

static CStorageExtension *GetExtension(duckdb_storage_extension extension) {
        if (!extension) {
                return nullptr;
        }
        auto wrapper = reinterpret_cast<CStorageExtensionWrapper *>(extension);
        if (!wrapper->extension) {
                return nullptr;
        }
        return wrapper->extension.get();
}

duckdb_storage_extension duckdb_create_storage_extension() {
        auto wrapper = new CStorageExtensionWrapper();
        wrapper->extension = duckdb::make_uniq<CStorageExtension>();
        return reinterpret_cast<duckdb_storage_extension>(wrapper);
}

void duckdb_destroy_storage_extension(duckdb_storage_extension *extension) {
        if (!extension || !*extension) {
                return;
        }
        auto wrapper = reinterpret_cast<CStorageExtensionWrapper *>(*extension);
        delete wrapper;
        *extension = nullptr;
}

void duckdb_storage_extension_set_extra_info(duckdb_storage_extension extension, void *extra_info,
                                             duckdb_delete_callback_t destroy) {
        auto ext = GetExtension(extension);
        if (!ext) {
                return;
        }
        ext->SetExtraInfo(extra_info, destroy);
}

void *duckdb_storage_extension_get_extra_info(duckdb_storage_extension extension) {
        auto ext = GetExtension(extension);
        if (!ext) {
                return nullptr;
        }
        return ext->GetExtraInfo();
}

void duckdb_storage_extension_set_attach(duckdb_storage_extension extension, duckdb_storage_attach_t attach) {
        auto ext = GetExtension(extension);
        if (!ext || !attach) {
                return;
        }
        ext->attach_callback = attach;
}

void duckdb_storage_extension_set_transaction(duckdb_storage_extension extension,
                                              duckdb_storage_create_transaction_t create_transaction) {
        auto ext = GetExtension(extension);
        if (!ext || !create_transaction) {
                return;
        }
        ext->create_transaction_manager_callback = create_transaction;
}

duckdb_state duckdb_config_add_storage_extension(duckdb_config config, const char *type,
                                                 duckdb_storage_extension extension) {
        if (!config || !type || !extension) {
                return DuckDBError;
        }
        auto db_config = reinterpret_cast<duckdb::DBConfig *>(config);
        auto wrapper = reinterpret_cast<CStorageExtensionWrapper *>(extension);
        if (!wrapper->extension) {
                return DuckDBError;
        }
        try {
                db_config->storage_extensions[type] = std::move(wrapper->extension);
        } catch (...) {
                return DuckDBError;
        }
        delete wrapper;
        return DuckDBSuccess;
}

static CAttachFunctionInfo *GetAttachInfo(duckdb_attach_info info) {
        if (!info) {
                return nullptr;
        }
        return reinterpret_cast<CAttachFunctionInfo *>(info);
}

static CTransactionInfo *GetTransactionInfo(duckdb_transaction_info info) {
        if (!info) {
                return nullptr;
        }
        return reinterpret_cast<CTransactionInfo *>(info);
}

const char *duckdb_attach_info_get_name(duckdb_attach_info info) {
        auto attach_info = GetAttachInfo(info);
        if (!attach_info) {
                return nullptr;
        }
        return attach_info->name.c_str();
}

const char *duckdb_attach_info_get_path(duckdb_attach_info info) {
        auto attach_info = GetAttachInfo(info);
        if (!attach_info) {
                return nullptr;
        }
        return attach_info->attach_info.path.c_str();
}

void duckdb_attach_info_set_path(duckdb_attach_info info, const char *path) {
        auto attach_info = GetAttachInfo(info);
        if (!attach_info || !path) {
                return;
        }
        attach_info->attach_info.path = path;
}

void duckdb_attach_info_set_duck_catalog(duckdb_attach_info info) {
        auto attach_info = GetAttachInfo(info);
        if (!attach_info) {
                return;
        }
        attach_info->catalog = duckdb::make_uniq<duckdb::DuckCatalog>(attach_info->db);
}

void duckdb_attach_info_set_error(duckdb_attach_info info, const char *error) {
        auto attach_info = GetAttachInfo(info);
        if (!attach_info) {
                return;
        }
        attach_info->success = false;
        attach_info->error = error ? error : string();
}

void *duckdb_attach_info_get_extra_info(duckdb_attach_info info) {
        auto attach_info = GetAttachInfo(info);
        if (!attach_info) {
                return nullptr;
        }
        return attach_info->extension.GetExtraInfo();
}

void duckdb_attach_info_get_client_context(duckdb_attach_info info, duckdb_client_context *out_context) {
        auto attach_info = GetAttachInfo(info);
        if (!attach_info || !out_context) {
                return;
        }
        try {
                auto wrapper = new duckdb::CClientContextWrapper(attach_info->context);
                *out_context = reinterpret_cast<duckdb_client_context>(wrapper);
        } catch (...) {
                *out_context = nullptr;
        }
}

static bool AttachInfoGetString(duckdb_attach_info info, const char *key, const char **out_value) {
        auto attach_info = GetAttachInfo(info);
        if (!attach_info || !key || !out_value) {
                return false;
        }
        auto value = attach_info->GetOptionValue(key);
        if (!value) {
                return false;
        }
        try {
                auto result = duckdb::StringValue::Get(value->DefaultCastAs(duckdb::LogicalType::VARCHAR));
                attach_info->string_cache.push_back(std::move(result));
                *out_value = attach_info->string_cache.back().c_str();
                return true;
        } catch (...) {
                return false;
        }
}

bool duckdb_attach_info_get_option_varchar(duckdb_attach_info info, const char *key, const char **out_value) {
        return AttachInfoGetString(info, key, out_value);
}

bool duckdb_attach_info_get_option_boolean(duckdb_attach_info info, const char *key, bool *out_value) {
        auto attach_info = GetAttachInfo(info);
        if (!attach_info || !key || !out_value) {
                return false;
        }
        auto value = attach_info->GetOptionValue(key);
        if (!value) {
                return false;
        }
        try {
                auto boolean_value = duckdb::BooleanValue::Get(value->DefaultCastAs(duckdb::LogicalType::BOOLEAN));
                *out_value = boolean_value;
                return true;
        } catch (...) {
                return false;
        }
}

bool duckdb_attach_info_get_option_bigint(duckdb_attach_info info, const char *key, int64_t *out_value) {
        auto attach_info = GetAttachInfo(info);
        if (!attach_info || !key || !out_value) {
                return false;
        }
        auto value = attach_info->GetOptionValue(key);
        if (!value) {
                return false;
        }
        try {
                auto bigint_value = duckdb::BigIntValue::Get(value->DefaultCastAs(duckdb::LogicalType::BIGINT));
                *out_value = bigint_value;
                return true;
        } catch (...) {
                return false;
        }
}

bool duckdb_attach_info_get_option_double(duckdb_attach_info info, const char *key, double *out_value) {
        auto attach_info = GetAttachInfo(info);
        if (!attach_info || !key || !out_value) {
                return false;
        }
        auto value = attach_info->GetOptionValue(key);
        if (!value) {
                return false;
        }
        try {
                auto double_value = duckdb::DoubleValue::Get(value->DefaultCastAs(duckdb::LogicalType::DOUBLE));
                *out_value = double_value;
                return true;
        } catch (...) {
                return false;
        }
}

void duckdb_transaction_info_set_duck_transaction_manager(duckdb_transaction_info info) {
        auto transaction_info = GetTransactionInfo(info);
        if (!transaction_info) {
                return;
        }
        transaction_info->transaction_manager = duckdb::make_uniq<duckdb::DuckTransactionManager>(transaction_info->db);
}

void duckdb_transaction_info_set_error(duckdb_transaction_info info, const char *error) {
        auto transaction_info = GetTransactionInfo(info);
        if (!transaction_info) {
                return;
        }
        transaction_info->success = false;
        transaction_info->error = error ? error : string();
}

void *duckdb_transaction_info_get_extra_info(duckdb_transaction_info info) {
        auto transaction_info = GetTransactionInfo(info);
        if (!transaction_info) {
                return nullptr;
        }
        return transaction_info->extension.GetExtraInfo();
}
