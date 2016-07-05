for each in *.py ; do
    echo $each::::::::::
    diff ../act5/$each $each
done
