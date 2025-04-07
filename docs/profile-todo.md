## SyncDB Profile Management Feature - TODO List

This list outlines the tasks required to implement the Profile Management feature as specified in `profile-prd.md`.

### Phase 1: Foundation & Configuration

*   [X] **Define Profile Struct:** Create a Go struct (`type ProfileConfig struct { ... }`) that accurately represents all the fields defined in PRD Section 5.1. (Done in `pkg/profile/profile.go`)
*   [X] **Implement Profile Path Logic:**
    *   [X] Create a function to resolve the profile directory path (checking `SYNCDB_PATH` env var, falling back to default `$HOME/.config/syncdb/profiles` or platform equivalent). (Done in `pkg/profile/profile.go` - `GetProfileDir`)
    *   [X] Implement logic to ensure the profile directory exists, creating it if necessary. Handle potential permissions errors. (Done in `pkg/profile/profile.go` - `GetProfileDir`)
*   [X] **Refactor Configuration Loading:** Modify the existing configuration loading mechanism (likely in `pkg/config`) to accommodate the new profile layer and respect the loading priority:
    1.  Command-line Flags
    2.  Environment Variables
    3.  Profile Settings (if `--profile` is used)
    4.  Application Defaults

### Phase 2: Profile Storage Operations

*   [X] **Implement Profile Read Function:** Create a function `loadProfile(profileName string) (*ProfileConfig, error)` that reads `<profileName>.yaml` from the profile directory and unmarshals it into the `ProfileConfig` struct. Handle file-not-found and parsing errors. (Done in `pkg/profile/profile.go` - `LoadProfile`)
*   [X] **Implement Profile Write Function:** Create a function `saveProfile(profileName string, config *ProfileConfig) error` that marshals the `ProfileConfig` struct to YAML and saves it to `<profileName>.yaml` in the profile directory. Handle file writing errors. (Done in `pkg/profile/profile.go` - `SaveProfile`)

### Phase 3: CLI Command Implementation (`pkg/cli`)

*   [X] **Add `profile` Parent Command:** Create the base `syncdb profile` command using Cobra.
*   [X] **Implement `profile create`:**
    *   [X] Define the `syncdb profile create <profile-name>` command using Cobra.
    *   [X] Add flags corresponding to all fields in `ProfileConfig`.
    *   [X] Implement logic:
        *   Check if profile already exists; fail if it does.
        *   Validate required flags (e.g., `--database`).
        *   Populate a `ProfileConfig` struct from flags.
        *   Call `saveProfile`.
        *   Provide user feedback (success/error messages).
    *   [X] Add warning about storing passwords in plain text if provided.
*   [X] **Implement `profile update`:**
    *   [X] Define the `syncdb profile update <profile-name>` command using Cobra.
    *   [X] Add the same flags as `profile create`.
    *   [X] Implement logic:
        *   Attempt to `loadProfile`. If it doesn't exist, start with an empty config (or treat like create).
        *   Merge flag values into the loaded/new config (only update fields provided by flags).
        *   Call `saveProfile`.
        *   Provide user feedback.
    *   [X] Add warning about storing passwords in plain text if provided/updated.
*   [X] **Implement `profile list`:**
    *   [X] Define the `syncdb profile list` command using Cobra.
    *   [X] Implement logic:
        *   Read the contents of the profile directory.
        *   Filter for files matching the profile naming convention (e.g., `*.yaml`).
        *   Extract profile names.
        *   (Optional Enhancement) Load key details (driver, database) for richer output.
        *   Print the list in a user-friendly format.
        *   Handle errors (e.g., directory not accessible).

### Phase 4: Integration with `export` and `import`

*   [ ] **Add `--profile` Flag:** Add the `--profile <profile-name>` string flag to both the `export` and `import` commands in Cobra.
*   [ ] **Integrate Profile Loading:** Modify the configuration setup logic within the `export` and `import` command handlers:
    *   If the `--profile` flag is provided:
        *   Call `loadProfile` using the flag value. Handle errors if the profile doesn't exist.
        *   Integrate the loaded `ProfileConfig` into the overall configuration settings according to the defined priority (Flags > Env Vars > Profile > Defaults).

### Phase 5: Testing

*   [ ] **Unit Tests (`pkg/config`):** Test profile path resolution and configuration merging logic.
*   [ ] **Unit Tests (`pkg/storage` - or similar for profile I/O):** Test profile reading and writing functions, including error cases.
*   [ ] **Integration Tests (CLI):**
    *   Test `profile create`, `update`, `list` commands with various flag combinations and edge cases (file exists/doesn't exist).
    *   Test `export --profile ...` and `import --profile ...`, ensuring profile settings are applied correctly.
    *   Test overriding profile settings with flags and environment variables.
    *   Test error handling for non-existent profiles.

### Phase 6: Documentation

*   [ ] **Update `README.md`:**
    *   Add a new section explaining the Profile Management feature.
    *   Document the new `profile create`, `update`, `list` commands with examples.
    *   Update `export` and `import` usage examples to show the `--profile` flag.
    *   Explain the profile storage location (`SYNCDB_PATH`, default path).
    *   Explain the configuration loading priority.
*   [ ] **Update CLI Help Text:** Ensure Cobra command descriptions and flag help messages are clear and accurate.
*   [ ] **Create `profile-prd.md`:** (Already done)
*   [ ] **Create `profile-todo.md`:** (This file)

### Future Enhancements (Post-Initial Release)

*   [ ] Implement `profile delete <profile-name>` command with confirmation.
*   [ ] Implement `profile show <profile-name>` command.
*   [ ] Add interactive mode for `profile create/update`.
*   [ ] Investigate profile encryption options.
