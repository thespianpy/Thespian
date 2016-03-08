#emacs -batch --load doc/thespian.el --visit=$1 --funcall org-export-as-pdf
emacs -batch --load doc/thespian.el --visit=$1 --funcall org-latex-export-to-pdf 2>&1 | tee doc/pdf.log
for x in 1 2 3 4 5 ; do
    (cd doc;
     texname=$(basename $1 .org).tex
     if grep 'Rerun to get cross-references right' pdf.log ; then
         pdflatex --interaction nonstopmode $texname | tee pdf.log
     fi
    )
done
