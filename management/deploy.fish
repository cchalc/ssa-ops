#!/usr/bin/env fish
# Deploy management notebooks to Databricks

set -l WORKSPACE_PROFILE "DEFAULT"
set -l TARGET_PATH "/Users/christopher.chalcraft@databricks.com/management"

echo "📦 Deploying management notebooks to Databricks..."
echo "   Profile: $WORKSPACE_PROFILE"
echo "   Target: $TARGET_PATH"
echo ""

# Get the directory where this script is located
set -l SCRIPT_DIR (dirname (status filename))

# Deploy each .py file
for notebook in $SCRIPT_DIR/*.py
    set -l name (basename $notebook)
    echo -n "  Deploying $name... "

    databricks workspace import "$TARGET_PATH/"(basename $notebook .py) \
        --file $notebook \
        --language PYTHON \
        --overwrite \
        --profile $WORKSPACE_PROFILE

    if test $status -eq 0
        echo "✓"
    else
        echo "✗"
    end
end

echo ""
echo "✓ Done! Notebooks available at:"
echo "  https://adb-984752964297111.11.azuredatabricks.net/#workspace$TARGET_PATH"
