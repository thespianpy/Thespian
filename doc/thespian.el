
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
         ;(plantuml . t)  ;; see org-babel-execute:plantuml below
         (python . t)
         (sh . t)
         (org . t)
         (latex . t))))

;; Note: this is extracted from ob-plantuml.el, but oriented instead
;; to the "plantuml" executable (script) instead of the jar file.
(defun org-babel-execute:plantuml (body params)
  "Execute a block of plantuml code with org-babel.
This function is called by `org-babel-execute-src-block'."
  (let* ((result-params (split-string (or (cdr (assoc :results params)) "")))
	 (out-file (or (cdr (assoc :file params))
		       (error "PlantUML requires a \":file\" header argument")))
	 (cmdline (cdr (assoc :cmdline params)))
	 (in-file (org-babel-temp-file "plantuml-"))
	 (cmd (concat "plantuml "
			(if (string= (file-name-extension out-file) "svg")
			    " -tsvg" "")
			(if (string= (file-name-extension out-file) "eps")
			    " -teps" "")
			" -p " cmdline " < "
			(org-babel-process-file-name in-file)
			" > "
			(org-babel-process-file-name out-file))))
    (with-temp-file in-file (insert (concat "@startuml\n" body "\n@enduml")))
    (message "%s" cmd) (org-babel-eval cmd "")
    nil)) ;; signal that output has already been written to file

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

(add-to-list 'load-path "${PWD}")
(load "${PWD}/doc/settings.el" 't)
