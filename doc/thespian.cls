%% Class definition for Thespian Documents for Latex
\NeedsTeXFormat{LaTeX2e}
\ProvidesClass{thespian}[2014/10/10 Thespian Documentation Class]
\newif\ifnoweb
\nowebfalse
\newif\ifbookfmt
\bookfmtfalse
\newif\ifusenormalchars
\usenormalcharsfalse
\bibliographystyle{tmbib}
\DeclareOption{noweb}{\nowebtrue}
\DeclareOption{book}{\bookfmttrue}
\DeclareOption{normalchars}{\usenormalcharstrue}
\DeclareOption{soppy}{%
        \typeout{Saw soppy option}%
        \typeout{Going on now...}}
\DeclareOption*{%
        \PassOptionsToClass{\CurrentOption}{article}%
        \PassOptionsToClass{\CurrentOption}{book}%
        }
\ProcessOptions\relax

\ifbookfmt
\LoadClass{book}
\newenvironment{abstract}{\hfill\begin{minipage}{4in}\hspace*{\fill}\bf ABSTRACT\hspace*{\fill}\\ \\}{\end{minipage}\hfill}
%\newenvironment{abstract}{\hspace{10ex}\begin{minipage}{4in}\hspace*{\fill}\bf ABSTRACT\hspace*{\fill}\\ \\}{\end{minipage}}
%\newenvironment{abstract}{\thanks{}{}}

\else

\LoadClass{article}
\newcounter{chapter}[part]
\renewcommand{\refname}{External References}

\fi

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% If we are documenting source code, we want noweb.
%%

\ifnoweb
\usepackage{noweb}
\typeout{Loaded Noweb}

% Our noweb customization settings
\def\nwendcode{\endtrivlist \endgroup} % let showcontextBelow do the \filbreak
\let\nwdocspar=\par

\setcodemargin{2em}
%\let\nowebsize=\small
\noweboptions{smallcode}

\fi

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% Font Selections
%%

%\usepackage{bookman}
%\usepackage{chancery}
%\usepackage{charter}
%\usepackage{newcent}
%\usepackage{palatino}
%\usepackage{utopia}

%\usepackage{times}
%\usepackage{mathptmx}
%\usepackage{courier}
%\usepackage{helvet}
%\usepackage{amssymb}
%\usepackage{symbol}

\usepackage{txfonts}


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% Common/Desired Supporting Packages
%%

%\usepackage{manpage}
\usepackage{fancyhdr}
\usepackage{longtable}
\usepackage{lastpage}
\usepackage{xspace}
%\usepackage{chappg}      % number pages with ``chap-page'' style
%\usepackage{varioref}    % elegant reference information
%\usepackage{prettyref}   % smarts about what is referenced
\usepackage{hyperref}    % make references into hypertext references

%\usepackage[rdkeywords]{listings}
%\lstloadlanguages{C}
%\lstset{language=C,basicstyle=\small,commentstyle=\itshape\footnotesize,texcl=true,indent=4em}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% PDF or non-PDF adaptations
%%

\newif\ifpdf
\ifx\pdfoutput\undefined
\pdffalse    % we are not running PDFLaTeX
\else
\pdfoutput=1    % we are running PDFLaTeX
\pdftrue
\fi

\ifpdf
\usepackage[pdftex]{graphicx}
\pdfcompresslevel=9
\else
\usepackage{graphicx}
\usepackage{ae}
\fi

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% Modify margins.  This section assumes US-standard letter-sized paper
%%                  and attempts to reduce the margins to use more of the
%%                  page for text.
%%

\setlength{\topmargin}{1pt}        %standard = 22pt
\setlength{\evensidemargin}{10pt}  %standard = 89pt
\setlength{\oddsidemargin}{10pt}   %standard = 35pt
\setlength{\marginparwidth}{46pt}  %standard = 125pt
\setlength{\textwidth}{460pt}      %standard = 345pt [424 from left]
\setlength{\textheight}{590pt}     %standard = 550pt, big=630pt
\addtolength{\headheight}{1.5ex}
\setlength{\itemsep}{-0.4em}
\setlength{\parindent}{0pt}
\setlength{\parskip}{1.0em}


%\newlength{tskip}
%\settoheight{1em

%\newfont{\fonta}{pnr10} % font is non-free so not included on all %systems


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% Page Header and Footer styling
%%

\fancypagestyle{title}{
\fancyhf{}  % clear all settings
\renewcommand{\headrulewidth}{0pt}
\tmpagefoot}

\def\tmpagehead{%
\fancyhead[LE,RO]{\it \leftmark}
\fancyhead[RE,LO]{\it \rightmark}
\renewcommand{\headrulewidth}{0.2pt}}

\def\tmpagefoot{%
\fancyfoot[LE,RO]{{\small\@docdate\ Rev. \@docrev}\\\thepage\ of \pageref{LastPage}}
\fancyfoot[RE,LO]{\small \@docid\\{\footnotesize PUBLIC DOMAIN}}
\fancyfoot[C]{{\small\@product\ \componentfooter\@doctype}\\ \includegraphics[scale=0.04]{thesplogo-only.jpg}
}
\renewcommand{\footrulewidth}{0.2pt}}

\pagestyle{fancy}
\fancyhf{}  % clear all settings
\tmpagehead
\tmpagefoot

% Redefine plain pages (used for chapters, etc)

\fancypagestyle{plain}{\fancyhf{}\tmpagehead\tmpagefoot}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% Generate title page with logo and additional information
%%


\renewcommand{\maketitle}{%
{
\gdef\what{\@product\xspace\@component\xspace}
\title{\@product\xspace\@component}
\ifpdf\pdfinfo{ /Title (\@product\ \@rawcomponent) /Subject(\@doctype) /Keywords(ID:\@docid\ rc\@docrev) /Author(\@author, \@tmgroup) }\fi
\onecolumn\fontsize{14}{14}\fontfamily{phv}\fontseries{b}%
\fontshape{n}\selectfont\thispagestyle{title}
\flushright
\vspace*{2.0em}
\includegraphics[scale=0.4]{thesplogo.jpg}
\\\@doctype~~
\vspace*{3.0em}
\hrule
\center
\ifx\@component\@empty
\@product\par%
%Version~\@version\par%
\else
\@product\par\@component\par%
%Version~\@version\par
\fi
%\vspace*{2em}
\vspace*{2em}
\hrule
\normalsize\normalfont\selectfont
By: \@author\hfill\@docdate\ (\#\@docrev)\par
\vspace*{6em}
\center
\@tmgroup\par\medskip
\ifx\@docid\@empty
\ \par
\else
\@docid \par
\fi
\medskip
{\bfseries PUBLIC DOCUMENT}
%\vspace{2em}
\vspace*{2em}
%\hrule
%%if uncommented, causes ``something's wrong'' errors: \normalsize\normalfont\flushleft
\normalsize\normalfont
\parskip 1em
\parindent 0em
%\vfill\eject
\markboth{Revision \@docrev}{\@component\ \@doctype}
}
}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% Document preface declarations: the following should appear in all
%% documents using this style file.
%%

%\newcommand{\subject}[1]{\title{#1}}
\newcommand{\subject}[1]{\product{#1}\message{Subject is Deprecated: Please use product and component instead.}}

\newcommand{\product}[1]{\def\@product{#1}}
\newcommand{\component}[2][\@component]{\def\@component{#2}\def\@rawcomponent{#1}}
\newcommand{\tmgroup}[1]{\def\@tmgroup{#1}}

\newcommand{\componentfooter}{
\ifx\@component\@empty\relax
\else\lbrack\@component\rbrack\xspace
\fi
}
\newcommand{\doctype}[1]{\def\@doctype{#1}}
\newcommand{\docrev}[2]{\def\@docrev{#1}\def\@docdate{#2}\date{#2}}
\newcommand{\docid}[1]{\def\@docid{#1}}

%% Defaults:
\product{No Subject/Product Defined}
\component{}
\doctype{Review}
\docrev{{\it not versioned}}{{\it date unknown}}
\docid{{\it uncontrolled}}
\tmgroup{Thespian Project}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% Optional Commands to generate section references for details
%%

\newcommand{\detailsA}[2]{#1 [details: Section \ref{#2}]}
\newcommand{\detailsB}[2]{\textsf{#1}\marginpar{\footnotesize\fbox{Section \ref{#2}}}}
\newcommand{\detailsC}[2]{\textup{#1~}\footnotesize\fbox{Section \ref{#2}}}
\newcommand{\details}[2]{\detailsC{#1}{#2}}


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% Optional dochistory Environment: Used to list the document's historical
%% info
%%

\newenvironment{dochistory}{\section*{Document Change History}
\begin{longtable}{||l|c|p{4.0in}||} \hline
\textbf{Date} & \textbf{Revision} & \textbf{Description} \\ \hline \hline
\endfirsthead
\hline
\multicolumn{3}{|c|}{\textit{Document Change History (continued)}} \\
\hline
\textbf{Date} & \textbf{Revision} & \textbf{Description} \\ \hline \hline
\endhead
}
{\hline \end{longtable}}

\newcommand{\histevent}[3]{{#1} & {#2} & {#3} \\ \hline}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% Optional docterms Environment: Used to list Terms and their Definitions
%%

\newenvironment{docterms}{\section*{Terms}
  
  The following terms are defined in the context of this document as
  having a specific meaning as defined here:

%tabular
\begin{longtable}{||r|p{3.75in}||} \hline
\textbf{Term} & \textbf{Definition} \\ \hline \hline}
{\hline \end{longtable}}
\newcommand{\docterm}[2]{{#1} & {#2} \\ \hline}


%%%%%%%% table2 environment: easy declaration of a table w/2 columns

\newenvironment{table2}[2]{%
  \begin{center}\begin{tabular}{ll}
      {\bf #1} & {\bf #2} \\ \hline%
  }{%
  \end{tabular}\end{center}}



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% Optional Variable and Register context tracking.  Useful with noweb
%% documentation of source code to specify and report the context (values)
%% of defined variables or numeric registers.
%% 

\newdimen\@cboxwidth \@cboxwidth=2.3in
\newdimen\@cboxparwidth \@cboxparwidth=\@cboxwidth
\newdimen\@cboxregwidth \@cboxregwidth=\@cboxwidth
\newdimen\@cboxlinewidth \@cboxlinewidth=\textwidth%hsize
\newdimen\@cboxregpwidth \@cboxregpwidth=\@cboxregwidth
\advance\@cboxparwidth by-1pt
\advance\@cboxregwidth by-3pt
\advance\@cboxregpwidth by-3em
\advance\@cboxlinewidth by-\@cboxwidth

\def\SetReg#1#2{\expandafter\def\csname context#1\endcsname{#2}}
\def\@ShowReg#1{ {\bf R#1} = \parbox[t]{\@cboxregpwidth}{\footnotesize\expandafter\csname context#1\endcsname}\\}
\def\@ShowRegs#1{%
  \def\@reglist{#1}%
  \@for\@regname:=\@reglist\do{%
    \@ShowReg{\@regname}}}

\def\@@ShowRegV#1{\expandafter\csname context#1\endcsname}

\newbox\@contextbox

\def\showcontext#1#2{\showcontextPARTSIZE{#1}{#2}}
\def\showcontextAbove#1#2{\showcontext{#1}{#2}\advance\dimen0 by-2ex\vspace{-\dimen0}}
\newsavebox{\showthiscontext}
\def\showcontextBelow#1#2{\setbox1 = \vtop{\showcontext{#1}{#2}}\advance\dimen0 by2ex\vspace{-\dimen0}\unvbox1\filbreak}

\newcommand{\showcontextFOO}[2]{\rule{10ex}{2pt}\setbox0 = \hbox{\setbox\@contextbox = \vtop{\fbox{\parbox{1.95in} {\parindent=-5em{\bf #1 CONTEXT:}\hrule\vspace{0.4pt}{\parbox{1.9in}{\@ShowRegs{#2}}}}}}}\dimen\@contextbox = \ht\@contextbox\unvbox\@contextbox\dimen0=\ht0\unhbox0\message{****CBHH: \the\dimen0 aswellas \the\dimen\@contextbox}\vspace{-\dimen0}}

\newcommand{\showcontextPARTSIZE}[2]{\setbox\@contextbox = \hbox{\fbox{\parbox{\@cboxparwidth} {\footnotesize{\bf #1 CONTEXT:}\hrule\vspace{5pt}{\parbox{\@cboxregwidth}{\@ShowRegs{#2}}}\vspace{-1ex}}}}\dimen\@contextbox = \ht\@contextbox\setbox0 = \vtop{\rule{\@cboxlinewidth}{0.5pt}\unhbox\@contextbox}\dimen0=\ht0\unvbox0}

\newcommand{\showcontextPART}[2]{\setbox\@contextbox = \hbox{\fbox{\parbox{1.95in} {{\bf #1 CONTEXT:}\hrule\vspace{3pt}{\parbox{1.9in}{\@ShowRegs{#2}}}}}}\dimen\@contextbox = \ht\@contextbox\setbox0 = \vtop{\rule{30em}{0.5pt}\unhbox\@contextbox}\dimen0=\ht0\unvbox0\message{****CBHH: \the\dimen0 aswellas \the\dimen\@contextbox}}%\vspace{-\dimen0}}


\newcommand{\showcontextCLOSE}[2]{\setbox\@contextbox = \hbox{\fbox{\parbox{1.95in} {{\bf #1 CONTEXT:}\hrule\vspace{0.4pt}{\parbox{1.9in}{\@ShowRegs{#2}}}}}}\dimen\@contextbox = \ht\@contextbox\setbox0 = \vtop{\hrulefill\unhbox\@contextbox}\dimen0=\ht0\unvbox0\message{****CBHH: \the\dimen0 aswellas \the\dimen\@contextbox}\vspace{-\dimen0}}

\newcommand{\showcontextALMOST}[2]{\setbox0 = \vtop{\setbox\@contextbox = \hbox{\fbox{\parbox{1.95in} {{\bf #1 CONTEXT:}\hrule\vspace{0.4pt}{\parbox{1.9in}{\@ShowRegs{#2}}}}}}}\dimen\@contextbox = \ht\@contextbox\hrulefill\unhbox\@contextbox\dimen0=\ht0\unvbox0\message{****CBHH: \the\dimen0 aswellas \the\dimen\@contextbox}\vspace{-\dimen0}}

\newcommand{\showcontextNOTQUITE}[2]{\setbox0 = \vtop{\setbox\@contextbox = \hbox{\fbox{\parbox{1.95in} {{\bf #1 CONTEXT:}\hrule\vspace{0.4pt}{\parbox{1.9in}{\@ShowRegs{#2}}}}}}\dimen\@contextbox = \ht\@contextbox\hfill\unhbox\@contextbox}\message{****CBH: \the\dimen\@contextbox}\unvbox0\vspace{-\dimen\@contextbox}}

\newcommand{\showcontextOK}[2]{\setbox\@contextbox = \hbox{\fbox{\parbox{1.95in} {{\bf #1 CONTEXT:}\hrule\vspace{0.4pt}{\parbox{1.9in}{\@ShowRegs{#2}}}}}}\dimen\@contextbox = \ht\@contextbox\hfill\unhbox\@contextbox\message{****CBH: \the\dimen\@contextbox}\vspace{-\dimen\@contextbox}}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

\def\SetVar#1#2{\expandafter\def\csname u@var@#1\endcsname{#2}}

\def\d@u@var@width{3em}
\def\ShowVarWidth#1{\expandafter\def\csname d@u@var@width\endcsname{#1}}
\def\ShowVar#1{\mbox{{\bf #1}=\parbox[t]{\d@u@var@width}{\footnotesize\expandafter\csname u@var@#1\endcsname}} }
\def\ShowVars#1{%
  \def\@vlist{#1}%
  \@for\@vname:=\@vlist\do{%
    \ShowVar{\@vname}\\}}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% Optional sequence chart environment.  This environment creates a
%% vertical message sequence chart where with labelled vertical lines
%% representing entities and then seqline specified events which occur
%% between the entities.
%%

% Declare a sequence chart, 
%    1st arg is number of entities,
%    2nd arg is the list of entity names (comma separated)
\def\seqline#1{%
  \def\@seqfields{#1}%
  \@for\@seqf:=\@seqfields\do{%
    \@seqf &} \\}
\def\@seqhdrline#1{%
  \def\@seqhdrs{#1}%
  \@for\@seqh:=\@seqhdrs\do{%
    \underline{\bf \@seqh} &} \\}
\newenvironment{seqchart}[2]
{\begin{center}\begin{tabular}{*{#1}{c}*{#1}{c}}a&b&c&d&e&f\\}%\@seqhdrline{#2}}
{\end{tabular}\end{center}}
%      \def\seqentnum{#1}%needed?
% Arg is a list of entity values/events, comma separated
%KWQ: right/left arrow fields?!

%Usage:
% \begin{seqchart}{3}{User,Cube,VAX}
% \seqline{\vline,\vline,\vline}
% \seqline{,,{\it Boot/Config}}
% \seqline{,,}
% \seqline{,,\discards}
% \end{seqchart}


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% Optional environments used to declare tables of parameters where the
%% format of the table is handled by these definitions and the source
%% document confines itself to describing the content.
%%

%paramtableA Environment: Used to list Configuration Parameters
%\paramA #1=Name, #2=Object Applies To, #3=Value,#4=Default
%\reqparamA ... same as \paramA but required
%\fundparamA ... same as \paramA but fundamental
%\reqfundparamA ... same as \paramA but fundamental and required
\newenvironment{paramtableA}[1]{
\begin{tabular}{llp{1in}p{1in}c}
\hline\multicolumn{5}{c}{\large\textbf{#1 Configuration Parameters}}\\&&&&\\
\textbf{Parameter} & \textbf{Object} & \textbf{Value} & 
\textbf{Default} &
\textbf{Flags}\footnote{Flags: R=Required, F=Fundamental} \\
  \hline
  \hline
}{\end{tabular}}
\newcommand\paramA[4]{\textbf{#1} & #2 & #3 & #4 & \\ \hline}
\newcommand\reqparamA[4]{\textbf{#1}&{#2}&{#3}&{#4}&R\\ \hline}
\newcommand\fundparamA[4]{\textbf{#1}&{#2}&{#3}&{#4}&F\\ \hline}
\newcommand\reqfundparamA[4]{\textbf{#1}&{#2}&{#3}&{#4}&RF\\ \hline}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%paramtableB Environment: Used to list Configuration Parameters
%\paramB #1=Name, #2=Object Applies To, #3=Value,#4=Default
%\reqparamB ... same as \paramB but required
%\fundparamB ... same as \paramB but fundamental
%\reqfundparamB ... same as \paramB but fundamental and required
\newenvironment{paramtableB}[1]{
\begin{longtable}{llp{2.24in}}
\hline\multicolumn{3}{c}{\large\textbf{#1 Configuration Parameters}}\\&&\\
\textbf{Parameter} & \textbf{Object} & \textbf{Value} \\
  \hline
  \hline
}
{\end{longtable}}
\newcommand{\paramB}[4]{\textbf{#1}&{#2}&{#3}\\&&\textit{Default:} {#4}\\ \hline}
\newcommand{\reqparamB}[4]{\textbf{#1}&{#2}&{#3}\\&&\textit{Default:} {#4}\\&&\textit{Required}\\ \hline}
\newcommand{\fundparamB}[4]{\textbf{#1}&{#2}&{#3}\\&&\textit{Default:} {#4}\\&&\textit{Fundamental}\\ \hline}
\newcommand{\reqfundparamB}[4]{\textbf{#1}&{#2}&{#3}\\&&\textit{Default:} {#4}\\&&\textit{Fundamental}\\&&\textit{Required}\\ \hline}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%paramtable Environment: Chosen to translate to paramtableN operations
\newenvironment{paramtable}[1]{\begin{paramtableB}{#1}}{\end{paramtableB}}
\newcommand{\param}[4]{\paramB{#1}{#2}{#3}{#4}}
\newcommand{\reqparam}[4]{\reqparamB{#1}{#2}{#3}{#4}}
\newcommand{\fundparam}[4]{\fundparamB{#1}{#2}{#3}{#4}}
\newcommand{\reqfundparam}[4]{\reqfundparamB{#1}{#2}{#3}{#4}}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% \code{xxx} environment used to typeset inline code.  Requires the
%% listings package (included above)
%%

%\newcommand{\code}[1]{\lstinline!#1!\xspace}


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% Miscellaneous/in-progress stuff
%%

%%\newenvironment{apidefs} {\list{dlk_foo}{\setlength{\leftmargin}{2cm}\setlength{\labelwidth}{1.8cm}\setlength{\parsep}{1em}\setlength{\itemsep}{2em}}}{}

%\newenvironment{apidefs}{\newpage}{\newpage}

%\newcommand{\apidef}[1]{\hrulefill \\} %subsubsection{\code{#1}}}

%\newcommand{\apirval}[1]{{\bf Returns:} #1}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%\newcommand{smalllist}{\begin{itemize}\setlength{\itemsep}{-0.4em}}{\end{itemize}}
%\def\smallitems{\list
%  \csname ITEM \endcsname{}}
%\def\smallitems{\list}
%\def\smallitems{\list\csname ITEM \endcsname{\def\makelabel##1{\hss\llap{##1}}}}
%\let\smallitems =\list\csname ITEM \endcsname{}
\def\smallitems{%
%  \vspace{-0.65em}
  \ifnum \@itemdepth >\thr@@\@toodeep\else
    \advance\@itemdepth\@ne
    \edef\@itemitem{labelitem\romannumeral\the\@itemdepth}%
    \expandafter
    \list
      \csname\@itemitem\endcsname
      {\def\makelabel##1{\hss\llap{##1}}}%
      \setlength{\topsep}{-0.4em}
%      \setlength{\parsep}{-1em}
      \addtolength{\itemsep}{-0.4em}
  \fi}
\let\endsmallitems =\endlist
%\newcommand{smalllist}{\begin{itemize}\setlength{\itemsep}{-0.4em}}{\end{itemize}}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% Redefine tableofcontents to toc to reduce the amount of
%% boilerplate code
%%

\newcommand{\toc}{\pagebreak{\parskip 0em\tableofcontents}}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% Redefine the underscore and dollarsign characters so that they are
%% just normal characters and don't cause special operations (to-wit:
%% subscripting and math-mode).  To achieve math-mode, use \(...\) markings.
%%

\ifusenormalchars
\catcode`\_=12%\other%11
%\def\_{_}
%\def\_#1{_#1}
\catcode`\$=\other
\fi

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
 %% Note environment.  Indented and boxed.
%%

\newdimen\@notemargin \@notemargin=1.0in
\newdimen\@noteboxwidth \@noteboxwidth=\textwidth
\advance\@noteboxwidth by-\@notemargin
\advance\@noteboxwidth by-\@notemargin
\newdimen\@notewidth \@notewidth=\@noteboxwidth
\advance\@notewidth by-2em

\def\note#1#2{\hspace*{\@notemargin}\(\longrightarrow\)\fbox{\parbox{\@notewidth}{{\bf #1:} #2}}}

