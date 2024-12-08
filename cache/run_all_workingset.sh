
python working_set_size.py uniform
mkdir res/working_set_size/uniform-rampdown
mv res/working_set_size/* res/working_set_size/uniform-rampdown

python working_set_size.py uniform-multi
mkdir res/working_set_size/uniform-multi
mv res/working_set_size/* res/working_set_size/uniform-multi

python working_set_size.py zipfian
mkdir res/working_set_size/zipfian-rampdown
mv res/working_set_size/* res/working_set_size/zipfian-rampdown

python working_set_size.py zipfian-multi
mkdir res/working_set_size/zipfian-multi
mv res/working_set_size/* res/working_set_size/zipfian-multi
