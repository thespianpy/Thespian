for each in *.py ; do
    echo $each::::::::::
    diff ../act3/$each $each
done
