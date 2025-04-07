## SyncDB Profile Management Feature PRD

**Version:** 1.0
**Date:** 2024-05-22
**Author:** AI Assistant based on user request

## 1. Introduction

SyncDB currently requires users to specify database connection details, export/import options, and storage configurations via command-line flags or environment variables for every operation. This can be repetitive and error-prone, especially when managing multiple database environments (e.g., development, staging, production) or complex configurations.

This document outlines the requirements for a new "Profile Management" feature for SyncDB. This feature will allow users to define, save, and reuse named configuration sets ("profiles") for common database synchronization tasks, simplifying the command-line usage for `export` and `import` operations.

## 2. Goals

*   **Improve User Experience:** Reduce the number of flags needed for common `export` and `import` tasks.
*   **Increase Efficiency:** Allow users to quickly switch between different database configurations.
*   **Reduce Errors:** Minimize typos and configuration mistakes by storing validated settings.
*   **Centralize Configuration:** Provide a structured way to manage different database sync setups.

## 3. Non-Goals

*   **Profile Encryption:** Sensitive information (like passwords) within profiles will be stored in plain text in the configuration files initially. Encryption is out of scope for this version.
*   **Remote Profile Storage:** Profiles will only be stored locally on the filesystem where SyncDB is run. Cloud synchronization or sharing of profiles is not included.
*   **Graphical User Interface (GUI):** Profile management will be exclusively through the command-line interface.
*   **Profile Versioning:** Managing different versions of the same profile is not a requirement.
*   **Automatic Profile Selection:** The tool will not automatically select a profile based on context; the user must explicitly specify it.

## 4. User Stories

*   **As a Developer,** I want to save the connection details and common export settings (like specific tables, excluding data) for my local development database, so I can easily export it using a simple command like `syncdb export --profile dev-local --storage local`.
*   **As an Operations Engineer,** I want to define profiles for staging and production databases, including specific tables and schema settings, so I can reliably run export/import tasks with consistent configurations.
*   **As a User,** I want to list all the profiles I have saved, so I can remember their names and what they are for.
*   **As a User,** I want to update an existing profile if database credentials or export requirements change.
*   **As a User,** I want command-line flags to override specific settings defined in a profile, so I can make temporary adjustments without modifying the saved profile (e.g., export only one table from a profile that usually exports several).

## 5. Functional Requirements

### 5.1. Profile Definition

*   A profile shall store a named set of configuration parameters.
*   The parameters stored within a profile **shall include**:
    *   `host`
    *   `port`
    *   `username`
    *   `password`
    *   `database`
    *   `driver`
    *   `tables` (comma-separated list or indication of 'all')
    *   `include-schema` (boolean)
    *   `include-data` (boolean)
    *   `condition` (string for WHERE clause)
    *   `exclude-table` (comma-separated list)
    *   `exclude-table-schema` (comma-separated list)
    *   `exclude-table-data` (comma-separated list)
*   The parameters stored within a profile **shall NOT include**:
    *   Storage type (`--storage`)
    *   Storage-specific settings (`--s3-bucket`, `--s3-region`, `--gdrive-folder`, `--folder-path` *for storage location*, `--file-path` *for import source*)
    *   Output format (`--format`) - *Decision: Keep format out of the profile for now, it relates more to the output artifact than the DB definition.*
    *   Import-specific behavior (`--upsert`) - *Decision: Keep upsert flag separate as it's an import-time decision.*

### 5.2. Profile Storage

*   Profiles shall be stored as individual files within a dedicated directory.
*   The location of this directory shall be determined by the `SYNCDB_PATH` environment variable.
*   If `SYNCDB_PATH` is not set, it shall default to `$HOME/.config/syncdb` (or platform equivalent).
*   Within the `SYNCDB_PATH` directory, profiles shall be stored under a `profiles/` subdirectory (e.g., `$HOME/.config/syncdb/profiles/`).
*   Each profile shall be stored in its own file, named `<profile-name>.yaml` (or potentially `.json` or `.toml` - YAML is preferred for readability).
*   The application must ensure the profile directory exists when creating/updating profiles.

### 5.3. CLI Commands

#### 5.3.1. `profile create`

*   **Command:** `syncdb profile create <profile-name> [flags...]`
*   **Action:** Creates a new profile configuration file named `<profile-name>.yaml` in the profile storage directory.
*   **Flags:** Accepts flags corresponding to *all* parameters listed in **Section 5.1** (Profile Definition).
    *   Example flags: `--host`, `--port`, `--username`, `--password`, `--database`, `--driver`, `--tables`, `--include-schema`, `--include-data`, `--condition`, `--exclude-table`, `--exclude-table-schema`, `--exclude-table-data`.
*   **Behavior:**
    *   Requires `<profile-name>` argument.
    *   Requires at least the `--database` flag to be meaningful. Other flags are optional and will use defaults if not provided (or remain unset in the profile file).
    *   If a profile with the same name already exists, the command shall fail with an error message, suggesting the use of `profile update`.
    *   Validates input flags where possible (e.g., port numbers).
    *   Saves the configuration to the corresponding file.

#### 5.3.2. `profile update`

*   **Command:** `syncdb profile update <profile-name> [flags...]`
*   **Action:** Modifies an existing profile configuration file or creates it if it doesn't exist.
*   **Flags:** Same flags as `profile create`.
*   **Behavior:**
    *   Requires `<profile-name>` argument.
    *   Loads the existing profile configuration (if any).
    *   Overrides existing values with any provided flags. Flags *not* provided do *not* change existing values in the profile.
    *   If the profile file does not exist, it creates a new one (similar to `create`).
    *   Saves the updated configuration to the file.

#### 5.3.3. `profile list`

*   **Command:** `syncdb profile list`
*   **Action:** Lists all available profiles found in the profile storage directory.
*   **Output:** Prints the names of the profiles found (e.g., the filenames without the extension). Optionally, could show key details like `driver` and `database` for each profile.
    ```
    Available Profiles:
    - dev-local (mysql, my_dev_db)
    - staging-main (postgres, my_staging_db)
    - prod-readonly (mysql, production_db)
    ```

#### 5.3.4. `profile delete` (Optional - Consider for future)

*   **Command:** `syncdb profile delete <profile-name>`
*   **Action:** Removes the specified profile file from the storage directory.
*   **Behavior:** Requires confirmation before deleting.

### 5.4. Using Profiles in `export` and `import`

*   **New Flag:** Introduce a `--profile <profile-name>` flag for both `export` and `import` commands.
*   **Action:** When `--profile` is used:
    1.  Load the configuration settings from the specified profile file.
    2.  Apply these settings as defaults for the operation.
*   **Configuration Loading Priority:** The final configuration for an operation shall be determined by the following precedence (highest priority first):
    1.  Command-line flags (e.g., `syncdb export --profile dev --port 3307` -> port 3307 is used, overriding the profile's port).
    2.  Environment variables (e.g., `SYNCDB_PORT=3308`) - *Note: Revisit if env vars should override profile or vice-versa. Standard practice often has flags > env vars > config files. Let's stick to Flag > Env Var > Profile > Default.*
    3.  Settings loaded from the `--profile <profile-name>` file.
    4.  Default values defined in the application code.
*   **Example Usage:**
    ```bash
    # Export using 'dev-local' profile settings, storing result in local ./backups folder
    syncdb export --profile dev-local --storage local --folder-path ./backups

    # Import using 'staging-main' profile, but override table selection for this run
    syncdb import --profile staging-main --tables users --storage s3 --s3-bucket my-staging-backups

    # Export using 'prod-readonly' profile, overriding password via env var
    export SYNCDB_PASSWORD="prod_password_override"
    syncdb export --profile prod-readonly --storage s3 --s3-bucket prod-backups
    ```

### 5.5. Error Handling

*   Error message if `--profile` specifies a profile that does not exist.
*   Error message if `profile create` is used for an existing profile.
*   Error messages for file system permission issues when reading/writing profile files.
*   Clear indication if a profile file is malformed or cannot be parsed.
*   Standard errors for invalid flag values during profile creation/update.

## 6. Technical Considerations

*   **Libraries:**
    *   Continue using Cobra for CLI structure.
    *   Use Viper or a similar library for handling configuration file loading (YAML/JSON/TOML) and merging with flags/env vars, respecting the defined priority.
*   **File Format:** YAML is recommended for profile files due to its readability.
*   **Directory Management:** Ensure robust creation and checking of the `SYNCDB_PATH` and `profiles/` directories. Handle different OS conventions for the default config path (`$HOME/.config/`).
*   **Password Handling:** Acknowledge that passwords are stored in plain text. Consider adding a warning during creation/update if a password is included.

## 7. Future Enhancements (Optional)

*   Implement `profile delete` command.
*   Add an interactive mode for `profile create` / `profile update` that prompts the user for values.
*   Introduce profile encryption using system keychains or a master password.
*   Add a `profile show <profile-name>` command to display the contents of a specific profile.
*   Allow profiles to optionally include *some* storage defaults if a strong use case emerges.
