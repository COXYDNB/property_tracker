import os
from collections import namedtuple
import yaml

# Full path to your project directory
FULL_PATH = "/Users/burit/Documents/VisualStudioCode/GIT/property_tracker/"

# Load settings from YAML
try:
    # First, try loading settings.yaml from the current directory
    with open("settings.yaml") as file:
        settings_yaml = yaml.load(file, Loader=yaml.SafeLoader)
except FileNotFoundError:  # If not found, load from FULL_PATH (for automatic scheduler)
    with open(os.path.join(FULL_PATH, "settings.yaml")) as file:
        settings_yaml = yaml.load(file, Loader=yaml.SafeLoader)
except yaml.YAMLError as e:  # Catch any YAML parsing errors
    raise RuntimeError(f"Error parsing settings.yaml: {e}")

# Convert YAML content to a named tuple
settings = namedtuple("Settings", settings_yaml.keys())(*settings_yaml.values())
