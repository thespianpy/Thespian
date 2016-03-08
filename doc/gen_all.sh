for X in doc/*.org ; do
    for F in html pdf text ; do
        echo "##### bash doc/gen_${F}.sh ${X} #####"
        bash doc/gen_${F}.sh ${X}
    done
done
rm -r ${TMPDIR}/{plantuml,dot,ditaa}-*
