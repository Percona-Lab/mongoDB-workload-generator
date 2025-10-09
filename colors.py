# Create or replace file: colors.py

class Bcolors:
    # Based on Percona Aqua Palette
    # Using ANSI 256-color escape codes for richer, more specific colors
    # Format: \033[38;5;<COLOR_CODE>m

    # Grays/Neutrals (for less critical info, or default text)
    GRAY_TEXT = '\033[38;5;242m' # A medium gray, similar to aqua-900 text but readable
    LIGHT_GRAY_TEXT = '\033[38;5;250m' # Very light gray for subtle details
    ORANGE = '\033[38;5;208m'
    
    # Standard Bright Colors (Added for compatibility)
    RED = '\033[38;5;196m'   # A standard bright red
    GREEN = '\033[38;5;40m'  # A standard bright green
    BLUE = '\033[94m'         # A standard bright blue

    # Aqua Shades for structure and main info
    # Lightest to Darkest Aqua/Green Shades
    AQUA_50  = '\033[38;5;195m'  # Very light, pale cyan
    AQUA_100 = '\033[38;5;158m'  # Light mint green
    AQUA_200 = '\033[38;5;121m'  # Pale seafoam green
    AQUA_300 = '\033[38;5;85m'   # Light seafoam green
    AQUA_400 = '\033[38;5;49m'   # Bright seafoam green
    AQUA_500 = '\033[38;5;36m'   # Core vibrant aqua
    AQUA_600 = '\033[38;5;30m'   # Richer, slightly darker aqua
    AQUA_700 = '\033[38;5;29m'   # Deeper teal
    AQUA_800 = '\033[38;5;23m'   # Dark teal/forest green
    AQUA_900 = '\033[38;5;22m'   # Very dark forest green

    # Specific use cases
    HEADER = AQUA_600      # Headers for sections
    STATS_HEADER = AQUA_700 # Header for each Collection Stats
    WORKLOAD_SETTING = AQUA_600 # Workload setting names
    SETTING_VALUE = AQUA_700 # The workload setting value
    HIGHLIGHT = AQUA_500   # Main throughput numbers, key results
    SECONDARY_HIGHLIGHT = LIGHT_GRAY_TEXT # This can be used to highlight something, but not as bright
    ACCENT = AQUA_600      # Table borders, separation lines
    WARNING = ORANGE # Orange for warnings (standard ANSI orange/gold)
    ERROR = RED      # Red for errors (standard ANSI red)
    DISABLED = GRAY_TEXT   # For settings that are disabled

    # --- Aliases for standard workload script ---
    FAIL = RED
    OKGREEN = GREEN
    OKCYAN = AQUA_400
    OKBLUE = BLUE

    # Styles
    ENDC = '\033[0m'       # Reset color
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'