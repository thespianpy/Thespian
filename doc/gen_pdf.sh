#emacs -batch --load doc/thespian.el --visit=$1 --funcall org-export-as-pdf
emacs -batch --load doc/thespian.el --visit=$1 --funcall org-latex-export-to-pdf
