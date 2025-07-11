# acmi_edw

A LakehousePlumber DLT pipeline project.

## Project Structure

- `pipelines/` - Pipeline configurations organized by pipeline name
- `presets/` - Reusable configuration presets
- `templates/` - Reusable action templates
- `substitutions/` - Environment-specific token and secret configurations
- `schemas/` - Schema definitions
- `expectations/` - Data quality expectations
- `generated/` - Generated DLT pipeline code

## Getting Started

1. Create a pipeline directory:
   ```bash
   mkdir pipelines/my_pipeline
   ```

2. Create a flowgroup YAML file:
   ```bash
   touch pipelines/my_pipeline/ingestion.yaml
   ```

3. Validate your configuration:
   ```bash
   lhp validate --env dev
   ```

4. Generate DLT code:
   ```bash
   lhp generate --env dev
   ```

## Commands

- `lhp validate` - Validate pipeline configurations
- `lhp generate` - Generate DLT pipeline code
- `lhp list-presets` - List available presets
- `lhp list-templates` - List available templates
- `lhp show <flowgroup>` - Show resolved configuration

For more information, visit: https://github.com/yourusername/lakehouse-plumber
