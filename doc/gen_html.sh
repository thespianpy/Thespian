emacs -batch --load doc/thespian.el --visit=$1 --funcall org-html-export-to-html
sed -i -e '/<title>/s|<a .*</a>|Thespian|' $(dirname $1)/$(basename $1 .org).html
