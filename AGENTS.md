# Instruction for coding agent

- Always use the `make` command if one exists for the task you're trying to accomplish (e.g. spining up infra through make instead of calling terraform directly)
- Make sure to always work within the .venv of the folder you are in, or of the Python file you're working on
- Whenever implementing Terraform changes, make sure you don't accidentally spawn thousands of resources which will cost us a pretty penny. Double-check your work to avoid accidentally explosions of billing