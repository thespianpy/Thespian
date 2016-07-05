for each in *.py ; do
    echo $each::::::::::
    diff ../act1/$each $each
done
