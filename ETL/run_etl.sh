#!/bin/zsh

# 1. Manually activate the Conda environment using its absolute path.
source "/opt/anaconda3/etc/profile.d/conda.sh"
conda activate reddit

# 2. Navigate to the project's root directory to ensure all relative paths work.
cd /Users/adam/Documents/Python/reddit_project_main/ETL
# 3. Run the main Python script.
python main.py