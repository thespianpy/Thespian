for each in *.py ; do
    echo $each::::::::::
    diff ../act2/$each $each
done
