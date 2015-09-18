
(require 'ox-latex)

  (add-to-list 'org-latex-classes
          '("thespian" "\\documentclass{thespian}
                       [DEFAULT-PACKAGES]
                       [EXTRA]"
                       ("\\section{%s}" . "\\section*{%s}")
                       ("\\subsection{%s}" . "\\subsection*{%s}")
                       ("\\subsubsection{%s}" . "\\subsubsection*{%s}")
                       ("\\paragraph{%s}" . "\\paragraph*{%s}")
                       ("\\subparagraph{%s}" . "\\subparagraph*{%s}")
                       ))

(setq org-src-preserve-indentation t)
(setq org-html-htmlize-output-type "css")
(setq org-html-htmlize-convert-nonascii-to-entities nil)
(setq org-html-htmlize-html-charset "utf-8")


(add-hook 'org-babel-after-execute-hook 'bh/display-inline-images 'append)
(defun bh/display-inline-images ()
  (condition-case nil
      (org-display-inline-images)
    (error nil)))
(org-babel-do-load-languages
 (quote org-babel-load-languages)
 (quote ((emacs-lisp . t)
         (dot . t)
         (ditaa . t)
         (plantuml . t)
         (python . t)
         (sh . t)
         (org . t)
         (latex . t))))

(setq org-confirm-babel-evaluate nil)

(setq org-image-actual-width 'nil)
(setq org-plantuml-jar-path (expand-file-name "~/.nix-profile/lib/plantuml.jar"))

(require 'ox-ascii)
(setq org-ascii-charset 'utf-8)

; Generate using.org PDF:
;   interactively via: Ctrl-c Ctrl-e p
;   cmdline: $ emacs -batch --load doc/thespian.el --visit=doc/using.org --funcall org-latex-export-to-pdf


; n.b. For Org < 8.0, the following changes should be made:
;    (require 'ox-latex)               ---> (require 'org-latex)
;    (add-to-list 'org-latex-classes   -->  (add-to-list 'org-export-latex-classes
;    -funcall org-latex-export-to-pdf  ---> -funcall org-export-as-pdf
