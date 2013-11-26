#
#  Purpose: Fill SNP postgres database from vcf output.
#			Run interactively as script prompts for user input
#			in case of repeated attempts to load the same file.
#
#-------------------------------------------------------------------------------------------
import psycopg2
import sys
import cyvcf
import re
import vcf


# Connects to the database and finds author_id from the Library table.
def get_author_id(librarycode):
    try:
        #Connecting to the database.
        dbh = psycopg2.connect(host='ngsdb', database='marea01', user='marea', password='marea')
        cur = dbh.cursor()
        try:
            cur.execute('SELECT author_id FROM "ngsdbview_library" WHERE librarycode = %s', (librarycode,))
            author_id = cur.fetchone()
            dbh.close()
            return author_id[0]
        except psycopg2.DatabaseError, e:
            #If an error occurs during the SELECT, the database will roll back any possible changes to the database.
            print 'Error %s' % e
            sys.exit(1)
    #If database cannot be loaded, print error message.
    except psycopg2.DatabaseError, e:
        print 'Error %s' % e
        sys.exit(1)


# Inserts statistic cvs from the INFO metadata into statistics cv when they are not already present.
# This information will be pulled to fill in the statistics table.
def insert_statistics_cv(infos, formats):
    #try:
    for cv_name, value in infos.iteritems():
        cv_definition = value[3]
        if cv_name != "EFF":
            #print cv_name
            dbh = psycopg2.connect(host='ngsdb', database="marea01", user='marea', password='marea')
            cur = dbh.cursor()
            try:
                cur.execute('SELECT cvterm_id FROM "ngsdbview_statistics_cv" WHERE cvterm = %s', (cv_name,))
                cv_id = cur.fetchone()
                if cv_id is not None:
                    dbh.commit()
                    dbh.close()
                #Inserts data into the database through 'execute'.
                else:
                    cur.execute('INSERT INTO "ngsdbview_statistics_cv" (cvterm, cv_notes) VALUES (%s, %s)',
                                (cv_name, cv_definition))
                    dbh.commit()
                    dbh.close()
            except psycopg2.IntegrityError:
                dbh.rollback()
    for cv_name, values in formats.iteritems():
        cv_definition = values[3]
        if cv_name != "EFF":

            dbh = psycopg2.connect(host='ngsdb', database="marea01", user='marea', password='marea')
            cur = dbh.cursor()
            try:
                cur.execute('SELECT cvterm_id FROM "ngsdbview_statistics_cv" WHERE cvterm = %s', (cv_name,))
                cv_id = cur.fetchone()
                if cv_id is not None:
                    dbh.commit()
                    dbh.close()
                #Inserts data into the database through 'execute'.
                else:
                    cur.execute('INSERT INTO "ngsdbview_statistics_cv" (cvterm, cv_notes) VALUES (%s, %s)',
                                (cv_name, cv_definition))
                    dbh.commit()
                    dbh.close()
            except psycopg2.IntegrityError:
                dbh.rollback()
                #except psycopg2.DatabaseError, e:
                #    print 'Error %s' % e
                #    sys.exit(1)


# Inserts the possible snp types into snp_type if they do not already exist.
# This information will be pulled for the snp_result table.
# May create redundant entries as it is unable to check for previous entries.
def insert_snp_type(indel, deletion, is_snp, monomorphic, transition, sv):
    try:
        dbh = psycopg2.connect(host='ngsdb', database="marea01", user='marea', password='marea')
        cur = dbh.cursor()
        try:
            cur.execute(
                'INSERT INTO "ngsdbview_snp_type" (indel, deletion, is_snp, monomorphic, transition, sv) VALUES (%s, %s, %s, %s, %s, %s) RETURNING snptype_id',
                (indel, deletion, is_snp, monomorphic, transition, sv))
            snptype_id = cur.fetchone()[0]
        except psycopg2.IntegrityError:
            dbh.rollback()
    except psycopg2.DatabaseError, e:
        print 'Error %s' % e
        sys.exit(1)
    dbh.commit()
    dbh.close()
    return snptype_id


# This table inserts each type of snp for the snp_result table. The snp_id is pulled from the snp_type table.
def insert_snp_results(snp_position, result_id, ref_base, alt_base, heterozygosity, quality, library_id, chromosome_id, snp_type):
    alt_bases = str(alt_base).strip('[]')
    try:
        dbh = psycopg2.connect(host='ngsdb', database="marea01", user='marea', password='marea')
        cur = dbh.cursor()
        try:
            print "Trying to Execute Insert"
            cur.execute(
                'INSERT INTO "ngsdbview_snp" (snp_position, result_id, ref_base, alt_base, heterozygosity, quality, library_id, chromosome_id, snptype_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING snp_id',
                (snp_position, result_id, ref_base, alt_bases, heterozygosity, quality, library_id, chromosome_id, snp_type))
            snp_id = cur.fetchone()[0]
            print "snp_id fetched"
            print snp_id
        except psycopg2.IntegrityError:
            print "Error in execute"
            dbh.rollback()
        #dbh.commit()
        #dbh.close()
        #return snp_id
    except psycopg2.DatabaseError, e:
        print 'Error %s' % e
        sys.exit(1)



# Inserts types of snp effects into effect_cv if they are not already listed.
# Effects are collected from the INFO metadata where id=EFF.
# Effect id will be pulled to fill in the effect table.
def insert_effect_cv(effect_list):
    try:
        dbh = psycopg2.connect(host='ngsdb', database="marea01", user='marea', password='marea')
        cur = dbh.cursor()
        effect_cv = re.findall('[\||\(][\s+](\w*)', effect_list)
        for each in effect_cv:
            try:
                cur.execute('SELECT * FROM "ngsdbview_effect_cv" WHERE effect_name = %s', (each,))
                effect_id = cur.fetchone()
                if effect_id is not None:
                    pass
                else:
                    cur.execute('INSERT INTO "ngsdbview_effect_cv" (effect_name) VALUES (%s)', (each,))
                    dbh.commit()
            except psycopg2.IntegrityError:
                dbh.rollback()
        dbh.close()
    except psycopg2.DatabaseError, e:
        print 'Error %s' % e
        sys.exit(1)


#def insert_effect(snp_id, effect_class, effect_string, effect_id):


# Checks to see if the database contains the chromosome. If it does, the database connection is closed.
# If the chromosome is not present, it is added into the database. This is done before adding any of the snp_results.
def insert_chromosome(chromosome_name, organismcode, genome_version):
    try:
        dbh = psycopg2.connect(host='ngsdb', database="marea01", user='marea', password='marea')
        cur = dbh.cursor()
        for each in chromosome_name:
            chromosome_fullname = each[0]
            size = each[1]
            chromosome = chromosome_fullname.split('_')[0]
            try:
                cur.execute('SELECT chromosome_id FROM "ngsdbview_chromosome" WHERE chromosome_name = %s AND genome_version = %s',
                            (chromosome, genome_version))
                chromosome_id = cur.fetchone()
                if chromosome_id is not None:
                    print "Chromosome is already in the database."
                else:
                    cur.execute('INSERT INTO "ngsdbview_chromosome" (chromosome_name, size, genome_name_id, genome_version) VALUES (%s, %s, %s, %s)',
                                (chromosome, size, organismcode, genome_version))
            except psycopg2.IntegrityError:
                dbh.rollback()
        dbh.commit()
        dbh.close()
    except psycopg2.DatabaseError, e:
        print 'Error %s' % e
        sys.exit(1)


# Identifies the chromosome ID if in the database. If not, prompts the user to add the chromosome.
def get_chromosome_id(chromosome, genome_version):
    try:
        dbh = psycopg2.connect(host='ngsdb', database="marea01", user='marea', password='marea')
        cur = dbh.cursor()
        try:
            chromosome_name = chromosome.split('_')[0]
            cur.execute('SELECT chromosome_id FROM "ngsdbview_chromosome" WHERE chromosome_name = %s AND genome_version = %s',
                        (chromosome_name, genome_version))
            chromosome_id = cur.fetchone()
            if chromosome_id is not None:
                return chromosome_id[0]
            else:
                print "Please add the chromosome to the ngsdb database."
        except psycopg2.IntegrityError:
            dbh.rollback()
        dbh.commit()
        dbh.close()
    except psycopg2.DatabaseError, e:
        print 'Error %s' % e
        sys.exit(1)


def get_organism_id(librarycode):
    try:
        dbh = psycopg2.connect(host='ngsdb', database="marea01", user='marea', password='marea')
        cur = dbh.cursor()
        try:
            cur.execute('SELECT organism_id FROM "ngsdbview_library" WHERE librarycode = %s',
                        (librarycode,))
            organism = cur.fetchone()
            dbh.close()
            if organism is not None:
                return organism[0]
            else:
                print "Please add organism to the ngsdb database."
                dbh.close()
        except psycopg2.IntegrityError:
            dbh.rollback()
    except psycopg2.DatabaseError, e:
        print 'Error %s' % e
        sys.exit(1)


def get_genome_id(organism_id, genome_version):
    try:
        dbh = psycopg2.connect(host='ngsdb', database="marea01", user='marea', password='marea')
        cur = dbh.cursor()
        try:
            cur.execute('SELECT genome_id FROM "ngsdbview_genome" WHERE organism_id = %s AND version = %s',
                        (organism_id, genome_version))
            genome_id = cur.fetchone()
            dbh.close()
            if genome_id is not None:
                return genome_id[0]
            else:
                print "Please add the genome to the ngsdb database."
        except psycopg2.IntegrityError:
            dbh.rollback()
        dbh.close()
    except psycopg2.DatabaseError, e:
        print 'Error %s' % e
        sys.exit(1)


def get_result(library_id, genome_id, author_id, analysisPath):
    try:
        dbh = psycopg2.connect(host='ngsdb', database="marea01", user='marea', password='marea')
        cur = dbh.cursor()
        try:
            cur.execute('SELECT result_id FROM "ngsdbview_result_libraries" WHERE library_id = %s', (library_id,))
            result = cur.fetchall()
            if result is not None:
                user_opt = input("There is already a result_id attached to this library. Please choose one of the"
                                 " following options."
                                 "\n1. Quit \n2. Override the old snp_results with these results. "
                                 "\n3. Keep old results and add these results under a new result_id. "
                                 "The old results will be marked as obsolete in the database.")
                result_num = 0
                for each in result:
                    result_id = each[0]
                    if user_opt == 1:
                        sys.exit("You have quit the program. SNP_Results were not uploaded into the database.")
                    elif user_opt == 2:
                        cur.execute('DELETE FROM "ngsdbview_snp" WHERE result_id = %s', (result_id,))
                    else:
                        is_current = True
                        is_obsolete = False
                        notes = ''
                        cur.execute('UPDATE "ngsdbview_result" SET is_current = %s, is_obsolete = %s WHERE result_id = %s', (False, True, result_id,))
                        if result_num == 0:
                            cur.execute(
                                'INSERT INTO "ngsdbview_result" (genome_id, author_id, is_current, is_obsolete, analysisPath, notes) VALUES (%s, %s, %s, %s, %s, %s) RETURNING result_id',
                                (genome_id, author_id, is_current, is_obsolete, analysisPath, notes,))
                            result_id = cur.fetchone()[0]
                            cur.execute('INSERT INTO "ngsdbview_result_libraries" (result_id, library_id) VALUES (%s, %s)',
                                        (result_id, library_id))
                            result_num += 1
                dbh.commit()
                dbh.close()
                return result_id
            else:
                is_current = True
                is_obsolete = False
                notes = ''
                cur.execute(
                    'INSERT INTO "ngsdbview_result" (genome_id, author_id, is_current, is_obsolete, analysisPath, notes) VALUES (%s, %s, %s, %s, %s, %s) RETURNING result_id',
                    (genome_id, author_id, is_current, is_obsolete, analysisPath, notes,))
                result_id = cur.fetchone()[0]
                cur.execute('INSERT INTO "ngsdbview_result_libraries" (result_id, library_id) VALUES (%s, %s)',
                            (result_id, library_id))
                dbh.commit()
                dbh.close()
                return result_id
        except psycopg2.IntegrityError:
            dbh.rollback()
        dbh.commit()
        dbh.close()
    except psycopg2.DatabaseError, e:
        print 'Error %s' % e
        sys.exit(1)


def get_heterozygosity(samples):
    for call in samples:
        heterozygosity = call.is_het
        return heterozygosity


# Identifies the library id from the Library table through the library code
def get_library_id(librarycode):
    try:
        dbh = psycopg2.connect(host='ngsdb', database="marea01", user='marea', password='marea')
        cur = dbh.cursor()
        try:
            cur.execute('SELECT library_id FROM "ngsdbview_library" WHERE librarycode = %s', [librarycode])
            library_id = cur.fetchone()
            dbh.close()
            if library_id is not None:
                return library_id[0]
            else:
                print "Please manually add the library to ngsdb."
        except psycopg2.IntegrityError:
            dbh.close()
    except psycopg2.DatabaseError, e:
        print 'Error %s' % e
        sys.exit(1)


def insert_snp_statistics(snp_id, cv_name, cv_value):
    try:
        dbh = psycopg2.connect(host='ngsdb', database="marea01", user='marea', password='marea')
        cur = dbh.cursor()
        try:
            cur.execute('SELECT cvterm_id FROM "ngsdbview_statistics_cv" WHERE cvterm = %s', (cv_name,))
            cvterm_id = cur.fetchone()
            if cvterm_id is not None:
                cur.execute('INSERT INTO "ngsdbview_statistics" (snp_id, stats_cvterm_id, cv_value) VALUES (%s, %s, %s)',
                            (snp_id, cvterm_id, cv_value))
        except psycopg2.IntegrityError:
            dbh.rollback()
        dbh.commit()
        dbh.close()
    except psycopg2.DatabaseError, e:
        print 'Error %s' % e
        sys.exit(1)


# Identifies the oragnism code from the Organism table through the organism id.
def get_organismcode(organism_id):
    try:
        dbh = psycopg2.connect(host='ngsdb', database="marea01", user='marea', password='marea')
        cur = dbh.cursor()
        try:
            cur.execute('SELECT organismcode FROM "ngsdbview_organism" WHERE organism_id = %s',
                        (organism_id,))
            organismcode = cur.fetchone()
            dbh.close()
            if organismcode is not None:
                return organismcode[0]
            else:
                print "Please add organism to the database."
        except psycopg2.IntegrityError:
            dbh.rollback()
    except psycopg2.DatabaseError, e:
        print 'Error %s' % e
        sys.exit(1)


#def insert_filter(snp_id, filter):
#    try:
#        dbh = psycopg2.connect(host='ngsdb', database="marea01", user='marea', password='marea')
#        cur = dbh.cursor()
#        try:
#            cur.execute('INSERT INTO "ngsdbview_filter" WHERE snp_id = %s AND filter_result = %s', (snp_id, filter,))
#            organismcode = cur.fetchone()
#            dbh.close()
#            if organismcode is not None:
#                return organismcode[0]
#            else:
#                print "Please add organism to the database."
#        except psycopg2.IntegrityError:
#            dbh.rollback()
#    except psycopg2.DatabaseError, e:
#        print 'Error %s' % e
#        sys.exit(1)


#def add_snp_effect():


def main():
    #Will ultimately be changed to allow vcf and summary files to be provided through the command line.
    number_of_files = input("Type 'yes' if you have both the vcf file and summary file. "
                            "Type 'no' if you only have the vcf file.")

    # If only a vcf file. Will be adjusted to automatically no through the command line input.
    if number_of_files == "no":
        print "Please note that without a summary file, the snp_summary & summary_level_cv table will not be updated. " \
              "This can be done manually at a later date"
        # collect and import vcf file.
        vcf_reader = vcf.Reader(open('/Volumes/mcobb$/Ld06_v01s1.vcf.gz.snpEff.vcf', 'r'))
        record = vcf_reader.next()

        # Collects input from the user.
        #--------------------------------------------------------------------

        # Identifies the librarycode, librarycode genome version,
        librarycode = input("Please state the librarycode.")
        genome_version = input("Please state the genome version")
        analysis_path = input('Please provide the full analysis path.')

        # SQL Inserts and Selects
        #----------------------------------------------------------------------
        # Collects the organism_id from librarycode
        organism_id = get_organism_id(librarycode)
        #organismcode = get_organismcode(organism_id)

        # Collects the genome_id from the Genome table
        genome_id = get_genome_id(organism_id, genome_version)

        # Collects the author_id from the Library table.
        author_id = get_author_id(librarycode)

        # Collects the library_id from the library table.
        library_id = get_library_id(librarycode)

        # Collects the result_id from results if already present, otherwise results are inserted into the table by
        # calling insert_result().
        result_id = get_result(library_id, genome_id, author_id, analysis_path)

        # Identifies the chromosome and chromosomal version
        #chromosome_name = vcf_reader.contigs.values()
        #insert_chromosome(chromosome_name, organismcode, genome_version)

        # Inserts Statistic CVs into the Statistics_CV.
        #info = vcf_reader.infos
        #formats = vcf_reader.formats
        #insert_statistics_cv(info, formats)

        # Inserts Effect types into effect_cv
        effect_list = vcf_reader.infos['EFF'].desc
        insert_effect_cv(effect_list)

        # Attributes that are unique for each SNP.
        for each in vcf_reader:
            ref_base = each.REF
            alt_base = each.ALT
            quality = each.QUAL
            filter = each.FILTER
            position = each.POS
            format = each.FORMAT
            is_snp = each.is_snp
            indel = each.is_indel
            deletion = each.is_deletion
            monomorphic = each.is_monomorphic
            sv = each.is_sv       # structural variant
            transition = each.is_transition
            statistics = each.INFO
            chromosome = each.CHROM
            effects = each.INFO['EFF']


            # Returns the heterozygosity of each snp.
            samples = each.samples
            heterozygosity = get_heterozygosity(samples)

            # Returns the chromosome_id for each snp result.
            chromosome_id = get_chromosome_id(chromosome, genome_version)

            # Inserts the SNP types.
            snptype_id = insert_snp_type(indel, deletion, is_snp, monomorphic, transition, sv)

            # Inserts each snp_results into the snp table
            snp_id = insert_snp_results(position, result_id, ref_base, alt_base, heterozygosity, quality, library_id, chromosome_id, snptype_id)

            # Inserts effects on each SNP into Effect. Need SNP_id
            #add_snp_effect(snp_id)

            # Inserts the snp's statistics into Statistics
            #for cv_name in statistics:
            #    cv_value = record.INFO[cv_name]
            #    insert_snp_statistics(snp_id, cv_name, cv_value)


            #if filter is not []:
            #    insert_filter(snp_id, filter)


    elif number_of_files == "yes":
        vcf_file = input("Please provide the full path of the vcf file.")
        sumamry_file = input("Please provide the full path of the summary file")

        # collect and import vcf file.
        vcf_reader = cyvcf.Reader(open('/Volumes/mcobb$/Ld06_v01s1.vcf.gz.snpEff.vcf', 'r'))
        record = vcf_reader.next()

    else:
        print "A vcf file is required for this program. Please try again."


main()


#----------------------------------------------------------------------------------
# Code snippets
#
#
#		cur.execute("SELECT * FROM author")
#		rows = cur.fetchall()
#		for row in rows:
#			print row
#
#
# def insertAnalysisType(analysisType):
#     for each in analysisType:
#         analysisInfo = each.split()
#         analysis_type = analysisInfo[0].split('=')[1]
#         definition = input("Please provide a definition for the following analysis type: " + analysis_type)
#         try:
#             dbh = psycopg2.connect(host='ngsdb', database="marea01", user='marea', password='marea')
#             cur = dbh.cursor()
#             #Inserts data into the database through 'execute'. Requires a tuple of values.
#             cur.execute('INSERT INTO "ngsdbview_analysistype" (analysis_type, definition) VALUES (%s, %s)',
#                         (analysis_type, definition))
#             dbh.commit()
#             dbh.close()
#         except psycopg2.DatabaseError, e:
#             print 'Error %s' % e
#             sys.exit(1)
#         finally:
#             if dbh:
#                 dbh.close()
