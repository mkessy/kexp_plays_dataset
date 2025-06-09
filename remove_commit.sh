#\!/bin/bash
git checkout main
# Create a temporary rebase script
echo "#\!/bin/bash" > /tmp/rebase_script.sh
echo "sed -i '' '/pick 11c53fc/d' \$1" >> /tmp/rebase_script.sh
chmod +x /tmp/rebase_script.sh

# Run interactive rebase with editor set to our script
GIT_SEQUENCE_EDITOR=/tmp/rebase_script.sh git rebase -i 11c53fc~1
