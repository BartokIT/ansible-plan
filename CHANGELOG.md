# Changelog

## Unreleased

### Added
- `meta.extends` names a base workflow file the workflow is merged over. Bases can extend other bases

### Changed
- A `_wf.yml` beside the workflow is now merged as a base instead of being prepended as text: mappings are merged key by key, so a workflow declaring its own `defaults` no longer loses those of `_wf.yml`, and files starting with `---` work. It is documented, and `extends: null` opts out of it
- `_wf.yml` is no longer applied to files pulled in with `include_block`

### Fixed
- Errors in a base file (invalid YAML, unknown keys, missing file, a file extending itself) are reported with its path

## 0.1.3 - 2025-11-14

### Fixed 
- Fixed doubtful mode issued
- Better question label 

## 0.1.2 - 2025-11-14

### Changed
- The cli choice to use the textual library is now 'visual'

### Fixed 
- Fixed strategy data visualized in the UI 
