for each in *.py ; do
    echo $each::::::::::
    diff ../act4/$each $each
done
