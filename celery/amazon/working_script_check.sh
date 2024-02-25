

main_script="parsers/eugene/parsers/amazon/scrape_amazon.py"

# function to check if the Python script is already running
is_script_running() {
    pgrep -f "$main_script" >/dev/null
}

# main logic to run the Python script only if it's not already running
if ! is_script_running; then
    echo "Python script ($main_script) is not running. Starting it now..."
    # Run your Python script here
    python3.11 "$main_script"
else
    echo "Python script ($main_script) is already running. Skipping..."
fi