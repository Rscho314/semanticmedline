%This module compiles the SemanticMEDLINE SQL database (in MariaDB) to a
% QLF, for further processing. This module only serves to build the QLF,
% and should not be executed routinely as part of the analysis.

:- module(
       sql_import,
       [compile_semmed_qlf/0]
   ).

:- use_module(library(odbc)).

% In the arguments of the table queries below, Titlecase is an atom
% (or timestamp), UPPERCASE is a number

sanitized_atom_codes(Atom, Codes) :- % Not relational, due to subtract
    subtract(Codes, [34,39], Sane_codes),
    append([39], Sane_tail, Atom_codes),
    append(Sane_codes, [39], Sane_tail),
    atom_codes(Atom, Atom_codes).

citations(citation(Pmid,Issn,Dp,Edat,PYEAR)) :-
    odbc_query(semmed, 'SELECT * FROM CITATIONS',
               row(PMID,ISSN,DP,EDAT,PYEAR),
              [types([atom, atom, atom, atom, integer])]),
    atomic_list_concat(['\'',PMID,'\''], Pmid),
    atomic_list_concat(['\'',ISSN,'\''], Issn),
    atomic_list_concat(['\'',EDAT,'\''], Edat),
    atomic_list_concat(['\'',DP,'\''], Dp).

generic_concepts(generic_concept(CONCEPT_ID,Cui,Preferred_name)) :-
    odbc_query(semmed, 'SELECT * FROM GENERIC_CONCEPT',
               row(CONCEPT_ID, CUI, PREFERRED_NAME),
              [types([integer, atom, codes])]),
    atomic_list_concat(['\'',CUI,'\''], Cui),
    sanitized_atom_codes(Preferred_name, PREFERRED_NAME).

predications(
    predication(PREDICATION_ID,SENTENCE_ID,Pmid,Predicate,Subject_cui,Subject_name,Subject_semtype,
                SUBJECT_NOVELTY,Object_cui,Object_name,Object_semtype,OBJECT_NOVELTY,Fact_value,
                Mod_scale,MOD_VALUE)
) :-
    odbc_query(
        semmed, 'SELECT * FROM PREDICATION',
        row(PREDICATION_ID,SENTENCE_ID,PMID,PREDICATE,
            SUBJECT_CUI,SUBJECT_NAME,SUBJECT_SEMTYPE,SUBJECT_NOVELTY,
            OBJECT_CUI,OBJECT_NAME,OBJECT_SEMTYPE,OBJECT_NOVELTY,
            FACT_VALUE,MOD_SCALE,MOD_VALUE),
        [types([integer,integer,atom,atom,atom,codes,codes,integer,atom,codes,codes,integer,codes,codes,float])]
    ),
    atomic_list_concat(['\'',PMID,'\''], Pmid),
    atomic_list_concat(['\'',PREDICATE,'\''], Predicate),
    atomic_list_concat(['\'',SUBJECT_CUI,'\''], Subject_cui),
    sanitized_atom_codes(Subject_name, SUBJECT_NAME),
    sanitized_atom_codes(Subject_semtype, SUBJECT_SEMTYPE),
    atomic_list_concat(['\'',OBJECT_CUI,'\''], Object_cui),
    sanitized_atom_codes(Object_name, OBJECT_NAME),
    sanitized_atom_codes(Object_semtype, OBJECT_SEMTYPE),
    sanitized_atom_codes(Fact_value, FACT_VALUE),
    sanitized_atom_codes(Mod_scale, MOD_SCALE).

predications_aux(
    predication_aux(PREDICATION_AUX_ID,PREDICATION_ID,Subject_text,SUBJECT_DIST,SUBJECT_MAXDIST,
                    SUBJECT_START_INDEX,SUBJECT_END_INDEX,SUBJECT_SCORE,Indicator_type,PREDICATE_START_INDEX,
                    PREDICATE_END_INDEX,Object_text,OBJECT_DIST,OBJECT_MAXDIST,OBJECT_START_INDEX,
                    OBJECT_END_INDEX,OBJECT_SCORE,Curr_timestamp)
) :-
    odbc_query(
        semmed, 'SELECT * FROM PREDICATION_AUX',
        row(PREDICATION_AUX_ID,PREDICATION_ID,SUBJECT_TEXT,SUBJECT_DIST,SUBJECT_MAXDIST,
            SUBJECT_START_INDEX,SUBJECT_END_INDEX,SUBJECT_SCORE,INDICATOR_TYPE,PREDICATE_START_INDEX,
            PREDICATE_END_INDEX,OBJECT_TEXT,OBJECT_DIST,OBJECT_MAXDIST,OBJECT_START_INDEX,
            OBJECT_END_INDEX,OBJECT_SCORE,CURR_TIMESTAMP),
        [types([integer,integer,codes,integer,integer,integer,integer,integer,atom,integer,integer,atom,integer,integer,integer,integer,integer,timestamp])]),
    sanitized_atom_codes(Subject_text, SUBJECT_TEXT),
    sanitized_atom_codes(Indicator_type, INDICATOR_TYPE),
    sanitized_atom_codes(Object_text, OBJECT_TEXT),
    CURR_TIMESTAMP =.. [_,Year,Month,Day,Hour,Minute,Second,_],
    date_time_stamp(date(Year, Month, Day, Hour, Minute, Second, 0, -, -), Curr_timestamp).

sentences(
    sentence(SENTENCE_ID,Pmid,Type,NUMBER,SENT_START_INDEX,SENT_END_INDEX,Section_header,
             Normalized_section_header,Sentence)
) :-
    odbc_query(semmed, 'SELECT * FROM SENTENCE',
               row(SENTENCE_ID,PMID,TYPE,NUMBER,SENT_START_INDEX,SENT_END_INDEX,SECTION_HEADER,
                   NORMALIZED_SECTION_HEADER,SENTENCE),
              [types([integer,atom,atom,integer,integer,integer,codes,codes,codes])]),
    atomic_list_concat(['\'',PMID,'\''], Pmid),
    atomic_list_concat(['\'',TYPE,'\''], Type),
    sanitized_atom_codes(Section_header, SECTION_HEADER),
    sanitized_atom_codes(Normalized_section_header, NORMALIZED_SECTION_HEADER),
    sanitized_atom_codes(Sentence, SENTENCE).


% CAUTION!! NULL IS 0 IN NUMBER COLS AND '0' IN ATOMIC COLS!!
compile_semmed_qlf :-
    % SemanticMEDLINE below is the name of the ODBC interface, not the name of the database
    odbc_connect('SemanticMEDLINE', _, [user(raoul), password('7320'), alias(semmed)]),
    odbc_set_connection(semmed, access_mode(read)),
    odbc_set_connection(semmed, null(0)), % NULL is redefined here
    tell('semmedVER41_2020_R.pl'),
    forall(citations(C), format('~w.~n', [C])),
    forall(generic_concepts(G), format('~w.~n', [G])),
    forall(predications(P), format('~w.~n', [P])),
    forall(predications_aux(PA), format('~w.~n', [PA])),
    forall(sentences(S), format('~w.~n', [S])),
    told,
    qcompile('semmedVER41_2020_R'),
    delete_file('semmedVER41_2020_R.pl'),
    odbc_disconnect(semmed).

























