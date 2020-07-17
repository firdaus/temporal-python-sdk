mkdir -p dependencies
cd dependencies
if [ -d "python-betterproto" ]; then
  n=$(ls -l|wc -l|sed -e 's/^[[:space:]]*//')
  echo $n
  mv python-betterproto "python-betterproto-$n"
fi
git clone git@github.com:firdaus/python-betterproto.git
cd python-betterproto
git checkout temporal-python-changes
pip install .[compiler]

