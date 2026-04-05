

for i in google-api-python-client xmltodict oauth2client pandas; do
	echo -n "Installing [$i]..."
	pip install --upgrade $i 1>/dev/null && echo "Done"
done

