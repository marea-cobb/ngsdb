#
#  Purpose: Fill SNP postgres database from vcf output.
#			Run interactively as script prompts for user input
#			in case of repeated attempts to load the same file.
#
#-------------------------------------------------------------------------------------------
import psycopg2
import sys
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
            dbh = psycopg2.connect(host='ngsdb', database="marea01", user='marea', password='marea')
            cur = dbh.cursor()
            try:
                cur.execute('SELECT stats_cvterm_id FROM "ngsdbview_statistics_cv" WHERE cvterm = %s', (cv_name,))
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
                cur.execute('SELECT stats_cvterm_id FROM "ngsdbview_statistics_cv" WHERE cvterm = %s', (cv_name,))
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
            cur.execute(
                'INSERT INTO "ngsdbview_snp" (snp_position, result_id, ref_base, alt_base, heterozygosity, quality, library_id, chromosome_id, snptype_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING snp_id', (snp_position, result_id, ref_base, alt_bases, heterozygosity, quality, library_id, chromosome_id, snp_type))
            snp_id = cur.fetchone()[0]
            dbh.commit()
            return snp_id
        except psycopg2.IntegrityError:
            print "Error in execute"
            dbh.rollback()
        dbh.close()
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


# Inserts the snp_effects for each snp. Each effect type is grouped using the group_id. Effect_id is specific to one vcf file.
# May need to be adjusted in the future.
def insert_effect(snp_id, effect_class, effect_strings, effect_group):
    effect_id = 1
    for effect_string in effect_strings:
        if effect_string:
            try:
                dbh = psycopg2.connect(host='ngsdb', database="marea01", user='marea', password='marea')
                cur = dbh.cursor()
                try:
                    cur.execute('INSERT INTO "ngsdbview_effect" (snp_id, effect_id, effect_class, effect_string, effect_group) VALUES (%s, %s, %s, %s, %s)',
                    (snp_id, effect_id, effect_class, effect_string, effect_group,))
                    dbh.commit()
                except psycopg2.IntegrityError:
                    dbh.rollback()
                dbh.close()
            except psycopg2.DatabaseError, e:
                print 'Error %s' % e
                sys.exit(1)
            effect_id += 1
        else:
            effect_id += 1
    dbh.close()


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
                    return chromosome_id[0]
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


# Identifies the organism_id from the Library table.
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


# #Collects the result_id
def get_result(library_id, genome_id, author_id, analysisPath):
    try:
        dbh = psycopg2.connect(host='ngsdb', database="marea01", user='marea', password='marea')
        cur = dbh.cursor()
        try:
            cur.execute('SELECT result_id FROM "ngsdbview_result_libraries" WHERE library_id = %s', (library_id,))
            result_ids = cur.fetchall()
            if result_ids:
                user_opt = input("There is already a result_id attached to this library. Please choose one of the"
                                 " following options."
                                 "\n1. Quit"
                                 "\n2. Override the old snp_results with these results. This will delete the old results. "
                                 "\n3. Keep old results and add these results under a new result_id. "
                                 "The old results will be marked as obsolete in the database. ")
                if user_opt == 1:
                        sys.exit("You have quit the program. SNP_Results were not uploaded into the database.")
                elif user_opt == 2:
                    notes = ''
                    print "you have chosen option 2"
                    for each in result_ids:
                        result_id = each[0]
                        cur.execute('DELETE FROM "ngsdbview_snp" WHERE result_id = %s', (result_id,))
                        dbh.commit()
                    cur.execute(
                        'INSERT INTO "ngsdbview_result" (genome_id, author_id, analysisPath, notes, is_current, is_obsolete) VALUES (%s, %s, %s, %s, %s, %s) RETURNING result_id',
                        (genome_id, author_id, analysisPath, notes, True, False))
                    print "Executed Insert"
                    result_id = cur.fetchone()[0]
                    cur.execute('INSERT INTO "ngsdbview_result_libraries" (result_id, library_id) VALUES (%s, %s)', (result_id, library_id))
                    dbh.commit()
                    return result_id
                elif user_opt == 3:
                    notes = ''
                    for each in result_ids:
                        result = each[0]
                        cur.execute('UPDATE "ngsdbview_result" SET is_current = %s, is_obsolete = %s WHERE result_id = %s', (False, True, result,))
                        dbh.commit()
                    print "committed updates"
                    cur.execute('INSERT INTO "ngsdbview_result" (genome_id, author_id, analysisPath, notes, is_current, is_obsolete) VALUES (%s, %s, %s, %s, %s, %s) RETURNING result_id',
                    (genome_id, author_id, analysisPath, notes, True, False))
                    print "Inserted results"
                    result_id = cur.fetchone()[0]
                    dbh.commit()
                    return result_id
            else:
                notes = ''
                cur.execute(
                    'INSERT INTO "ngsdbview_result" (genome_id, author_id, analysisPath, notes, is_current, is_obsolete) VALUES (%s, %s, %s, %s, %s, %s) RETURNING "result_id"',
                    (genome_id, author_id, analysisPath, notes, True, False))
                result_id = cur.fetchone()[0]
                cur.execute('INSERT INTO "ngsdbview_result_libraries" (result_id, library_id) VALUES (%s, %s)', (result_id, library_id))
                dbh.commit()
                return result_id
        except psycopg2.IntegrityError:
            dbh.rollback()
    except psycopg2.DatabaseError, e:
        print 'Error %s' % e
        sys.exit(1)


# Determines if the snp is heterozygous. Built in pyvcf function.
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
            cur.execute('SELECT cvterm FROM "ngsdbview_statistics_cv" WHERE cvterm = %s', (cv_name,))
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
        

# Inserts any filters that a snp failed on.
def insert_filter_cv(filter_type):
    try:
        dbh = psycopg2.connect(host='ngsdb', database="marea01", user='marea', password='marea')
        cur = dbh.cursor()
        try:
            if filter_type:
                filter_string = filter_type[0]
                cur.execute('SELECT "filter_cv_id" FROM "ngsdbview_filter_cv" WHERE filter_type = %s', (filter_string,))
                filterCV_id = cur.fetchone()
                if filterCV_id is None:
                    cur.execute('INSERT INTO "ngsdbview_filter_cv" (filter_type) VALUES (%s) RETURNING "filter_cv_id"', (filter_string,))
                    filterCV_id = cur.fetchone()
                dbh.commit()
                dbh.close()
                return filterCV_id
        except psycopg2.IntegrityError:
            dbh.rollback()
    except psycopg2.DatabaseError, e:
        print 'Error %s' % e
        sys.exit(1)


# Inserts filters into the Filter table. Always inserts the filter as Failed.
def insert_filter(snp_id, filter_cv_id):
    try:
        dbh = psycopg2.connect(host='ngsdb', database="marea01", user='marea', password='marea')
        cur = dbh.cursor()
        filter_result = False
        try:
            cur.execute('INSERT INTO "ngsdbview_filter" (snp_id, filter_result, filter_cv_id) VALUES (%s, %s, %s)', (snp_id, filter_result, filter_cv_id[0],))
            dbh.commit()
        except psycopg2.IntegrityError:
            dbh.rollback()
    except psycopg2.DatabaseError, e:
        print 'Error %s' % e
        sys.exit(1)
    #cur.execute('SELECT * FROM "ngsdbview_filter"')

    
    
def main():
    # Reads the file in from the command line. First file is the script, second is the vcf file,
    # and an option second is the summary file.
    num_of_files = len(sys.argv[1:])
    vcf_file = sys.argv[1]

    # If only a vcf file. Will be adjusted to automatically no through the command line input.
    if num_of_files == 1:
        print "Please note that without a summary file, the snp_summary & summary_level_cv table will not be updated. " \
              "This can be done manually at a later date"
        # collect and import vcf file.
        vcf_reader = vcf.Reader(open(vcf_file, 'r'))
        record = vcf_reader.next()

        # Collects input from the user.
        #--------------------------------------------------------------------

        # Identifies the librarycode, librarycode, genome_id, and genome version,
        librarycode = raw_input("Please state the librarycode. ")
        print "genome_id\t organismcode\t genome_version"
        try:
            dbh = psycopg2.connect(host='ngsdb', database="marea01", user='marea', password='marea')
            cur = dbh.cursor()
            try:
                cur.execute('SELECT genome_id, organism_id, version FROM "ngsdbview_genome"s',
                            (librarycode,))
                genome = cur.fetchall()
                for each in genome:
                    try:
                        cur.execute('SELECT organismcode FROM "ngsdbview_organism" WHERE organism_id=%s', (each[1],))
                        organismcode = cur.fetchone()
                        print "{0}\t{1}\t{2}".format(each[0], organismcode[0], each[2])
                    except psycopg2.IntegrityError:
                        dbh.rollback()
            except psycopg2.IntegrityError:
                dbh.rollback()
        except psycopg2.DatabaseError, e:
            print 'Error %s' % e
            sys.exit(1)

        genome_id = raw_input("Please state the genome_id. ")
        genome_version = raw_input("Please state the genome version. ")
        analysis_path = raw_input("Please provide the full analysis path. ")

        # SQL Inserts and Selects
        #----------------------------------------------------------------------
        # Collects the organism_id from librarycode
        organism_id = get_organism_id(librarycode)
        organismcode = get_organismcode(organism_id)

        # Collects the genome_id from the Genome table
        #genome_id = get_genome_id(organism_id, genome_version)

        # Collects the author_id from the Library table.
        author_id = get_author_id(librarycode)
        print "Got author_id."

        # Collects the library_id from the library table.
        library_id = get_library_id(librarycode)

        # Collects the result_id from results if already present, otherwise results are inserted into the table by
        # calling insert_result().
        result_id = get_result(library_id, genome_id, author_id, analysis_path)
        print "Got result_id"

        # Identifies the chromosome and chromosomal version
        chromosome_name = vcf_reader.contigs.values()
        insert_chromosome(chromosome_name, organismcode, genome_version)
        print "Chromosome inserted"

        # Inserts Statistic CVs into the Statistics_CV.
        info = vcf_reader.infos
        formats = vcf_reader.formats
        insert_statistics_cv(info, formats)
        print "Statistics inserted"

        # Inserts Effect types into effect_cv
        effect_list = vcf_reader.infos['EFF'].desc
        insert_effect_cv(effect_list)
        print "Effect inserted"

        # Attributes that are unique for each SNP.
        for each in vcf_reader:
            ref_base = each.REF
            alt_base = each.ALT
            quality = each.QUAL
            filter_type = each.FILTER
            position = each.POS
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
            print "Got chromosome_id"

            # Inserts the SNP types.
            snptype_id = insert_snp_type(indel, deletion, is_snp, monomorphic, transition, sv)
            print "Got snptype_id"

            # Inserts each snp_results into the snp table
            snp_id = insert_snp_results(position, result_id, ref_base, alt_base, heterozygosity, quality, library_id, chromosome_id, snptype_id)
            print "Got Snp_id"

            # Inserts effects on each SNP into Effect.
            group_id = 0
            for each in effects:
                group_id += 1
                effect_class = each.split('(')[0]
                effect_string = re.split('\((\S*\|\S*)\)', each)[1]
                effects_string = effect_string.split('|')
                insert_effect(snp_id, effect_class, effects_string, group_id)
            print "Got effects"

            # Inserts the snp's statistics into Statistics
            for cv_name in statistics:
                cv_value = statistics[cv_name]
                if isinstance(cv_value, list):
                    for each in cv_value:
                        insert_snp_statistics(snp_id, cv_name, each)
                else:
                    insert_snp_statistics(snp_id, cv_name, cv_value)
            print "Got statistics"

            # Checks to see if the snp failed on a filter. If so then inserts into filter table.
            if filter_type:
                filter_cv_id = insert_filter_cv(filter_type)
                insert_filter(snp_id, filter_cv_id)
            print "Got filters"

    # NEED A STANDARD SUMMARY FILE
    elif num_of_files == 2:
        # collect and import vcf file.
        vcf_reader = vcf.Reader(open(vcf_file, 'r'))
        record = vcf_reader.next()

    else:
        print "A vcf file is required for this program. Please try again."


main()