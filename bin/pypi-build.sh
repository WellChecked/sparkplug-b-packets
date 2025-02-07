if [ -d dist ]
then
  rm dist/*
fi
python3 -m build