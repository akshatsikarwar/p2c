#include <arpa/inet.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>

#include <cdb2api.h>
#include <libpq-fe.h>
#include <server/catalog/pg_type_d.h>

static int finish;
static PGconn *pg;
static char *insert;

#define QUOTE(x) #x
#define STR(x) QUOTE(x)

#define THDS 8
#define INSERTS 1000

struct insert_arg {
    int id;
    pthread_mutex_t lk;
    pthread_cond_t cond;
    pthread_t thd;
    int rows;
    char *val[INSERTS][165];
    int len[INSERTS][165];
    cdb2_hndl_tp *hndl;
};

static void *insert_func(void *arg)
{
    struct insert_arg *data = arg;
    cdb2_hndl_tp *cdb2 = data->hndl;
    int rc = cdb2_open(&cdb2, "ets", "c2btda-pw-672", CDB2_DIRECT_CPU);
    if (rc != 0) {
        fprintf(stderr, "cdb2_open rc:%d err:%s\n", rc, cdb2_errstr(cdb2));
        abort();
    }

    pthread_mutex_lock(&data->lk);
    while (1) {
        pthread_cond_wait(&data->cond, &data->lk);
        if (finish) {
            pthread_mutex_unlock(&data->lk);
            return NULL;
        }
        if ((rc = cdb2_run_statement(cdb2, "begin")) != 0) {
            fprintf(stderr, "cdb2_run_statement -- begin -- rc:%d err:%s\n", rc, cdb2_errstr(cdb2));
            abort();
        }
        for (int row = 0; row < data->rows; ++row) {
            for (int col = 0; col < 165; ++col) {
                cdb2_bind_index(cdb2, col + 1, CDB2_CSTRING, data->val[row][col], data->len[row][col]);
            }
            if ((rc = cdb2_run_statement(cdb2, insert)) != 0) {
                fprintf(stderr, "cdb2_run_statement -- insert -- rc:%d err:%s\n", rc, cdb2_errstr(cdb2));
                abort();
            }
            cdb2_clearbindings(cdb2);
        }
        if ((rc = cdb2_run_statement(cdb2, "commit")) != 0) {
            fprintf(stderr, "cdb2_run_statement -- commit -- rc:%d err:%s\n", rc, cdb2_errstr(cdb2));
            if (rc != CDB2ERR_DUPLICATE) abort();
            abort();
        }
    }
}

static void make_ins_sql(PGresult *res)
{
    char *sql = malloc(10 * 1024);
    char *vals = malloc(5 * 1024);
    sprintf(sql, "INSERT INTO trade_store_fi(");
    vals[0] = 0;
    int cols = PQnfields(res);
    const char *comma = "";
    for (int c = 0; c < cols; ++c) {
        char name[512];
        sprintf(name, "%s\"%s\"", comma, PQfname(res, c));
        strcat(sql, name);
        sprintf(name, "%s?", comma);
        strcat(vals, name);
        comma = ", ";
    }
    strcat(sql, ") values(");
    strcat(sql, vals);
    strcat(sql, ");");
    free(vals);
    insert = sql;
}

int main(int argc, char **argv)
{
    setenv("COMDB2TZ", "UTC", 1);
    void sighold(int);
    sighold(SIGPIPE);

    int row = 0;
    int first = 1;
    int stripe = 0;
    struct insert_arg *data = calloc(THDS, sizeof(struct insert_arg));
    for (int i = 0; i < THDS; ++i) {
        data[i].id = i;
        pthread_cond_init(&data[i].cond, NULL);
        pthread_mutex_init(&data[i].lk, NULL);
        pthread_create(&data[i].thd, NULL, insert_func, &data[i]);
    }

    pg = PQsetdbLogin("citusqa-co-f7e609.database-lab-daas.dev.query.bms.bloomberg.com", "10108", NULL, NULL, NULL, "postgres", "P0stG&e$");
    if (!pg) {
        fprintf(stderr, "PQsetdbLogin err:%s\n", PQerrorMessage(pg));
        return EXIT_FAILURE;
    }
    ConnStatusType conn_status = PQstatus(pg);
    switch (conn_status) {
    case 0: break;
    case CONNECTION_STARTED:            puts("CONNECTION_STARTED"); break;
    case CONNECTION_MADE:               puts("CONNECTION_MADE"); break;
    case CONNECTION_AWAITING_RESPONSE:  puts("CONNECTION_AWAITING_RESPONSE"); break;
    case CONNECTION_AUTH_OK:            puts("CONNECTION_AUTH_OK"); break;
    case CONNECTION_SSL_STARTUP:        puts("CONNECTION_SSL_STARTUP"); break;
    case CONNECTION_SETENV:             puts("CONNECTION_SETENV"); break;
    case CONNECTION_CHECK_WRITABLE:     puts("CONNECTION_CHECK_WRITABLE"); break;
    case CONNECTION_CONSUME:            puts("CONNECTION_CONSUME"); break;
    default:                            fprintf(stderr, "PQstatus unknown status:%d err:%s\n", conn_status, PQerrorMessage(pg)); return EXIT_FAILURE;                                        
    }

    PQclear(PQexec(pg, "set schema 'ets';"));

    int total = 0;

    int from_month = atoi(argv[1]); 
    int from_year = atoi(argv[2]);
    int to_month = atoi(argv[3]); 
    int to_year = atoi(argv[4]);

    while (1) {
        if (from_month == to_month && from_year == to_year) {
            printf("stopping at %04d-%02d\n", from_year, from_month);
            break;
        }
        char sql[512];
        sprintf(sql, "SELECT * FROM trade_store_fi_%02d%d;", from_month, from_year);
        puts(sql);
        if (!PQsendQuery(pg, sql)) {
            fprintf(stderr, "PQsendQuery failed err:%s\n", PQerrorMessage(pg));
            goto end;
        }
        PQsetSingleRowMode(pg);
        PGresult *res = PQgetResult(pg); 
        while (res) {
        const char *status_str = "???";
        int status = PQresultStatus(res);
        switch (status) {
        case PGRES_BAD_RESPONSE     : status_str = "PGRES_BAD_RESPONSE"; break;
        case PGRES_COMMAND_OK       : status_str = "PGRES_COMMAND_OK"; break;
        case PGRES_COPY_BOTH        : status_str = "PGRES_COPY_BOTH"; break;
        case PGRES_COPY_IN          : status_str = "PGRES_COPY_IN"; break;
        case PGRES_COPY_OUT         : status_str = "PGRES_COPY_OUT"; break;
        case PGRES_EMPTY_QUERY      : status_str = "PGRES_EMPTY_QUERY"; break;
        case PGRES_FATAL_ERROR      : status_str = "PGRES_FATAL_ERROR"; break;
        case PGRES_NONFATAL_ERROR   : status_str = "PGRES_NONFATAL_ERROR"; break;
        case PGRES_PIPELINE_ABORTED : status_str = "PGRES_PIPELINE_ABORTED"; break;
        case PGRES_PIPELINE_SYNC    : status_str = "PGRES_PIPELINE_SYNC"; break;
        case PGRES_SINGLE_TUPLE     : status_str = "PGRES_SINGLE_TUPLE"; break;
        case PGRES_TUPLES_OK        : status_str = "PGRES_TUPLES_OK"; break;
        }
        if (status != PGRES_TUPLES_OK && status != PGRES_SINGLE_TUPLE) {
            fprintf(stderr, "PQgetResult failed res:%p status:%d (%s) err:%s (%s)\n", res, status, status_str, PQerrorMessage(pg), PQresultErrorMessage(res));
            while (res) {
                PQclear(res);
                res = PQgetResult(pg);
            }
            break;
        }
        if (first) {
            first = 0;
            if (PQnfields(res) != 165) {
                fprintf(stderr, "bad number of columns:%d\n", PQnfields(res));
                abort();
            }
            make_ins_sql(res);
        }
        for (int pgrow = 0; pgrow < PQntuples(res); ++pgrow) {
            ++total;
            if (row == 0) {
                pthread_mutex_lock(&data[stripe].lk);
            }
            for (int col = 0; col < 165; ++col) {
                int null = PQgetisnull(res, pgrow, col);
                if (null) {
                    if (col == 0) {
                        fprintf(stderr, "something is wrong -- id can't be null\n");
                        abort();
                    }
                    free(data[stripe].val[row][col]);
                    data[stripe].val[row][col] = NULL;
                    data[stripe].len[row][col] = 0;
                    continue;
                }
                int len = PQgetlength(res, pgrow, col);
                if (data[stripe].len[row][col] < len) {
                    data[stripe].val[row][col] = realloc(data[stripe].val[row][col], len + 1);
                    data[stripe].len[row][col] = len;
                }
                Oid type = PQftype(res, col);
                char *val = PQgetvalue(res, pgrow, col);
                switch (type) {
                case TIMESTAMPOID: val[10] = 'T'; /* fall through */
                default: strcpy(data[stripe].val[row][col], val); break;
                }
            }
            ++row;
            if (row % INSERTS == 0) {
                data[stripe].rows = row;
                row = 0;
                pthread_cond_signal(&data[stripe].cond);
                pthread_mutex_unlock(&data[stripe].lk);
                ++stripe;
                if (stripe == THDS) stripe = 0;
            }
        }
        PQclear(res);
        res = PQgetResult(pg); 
        } // while (res)
end:    ++from_month;
        if (from_month > 12) {
            from_month = 1;
            ++from_year;
        }
    } // while (1)

    if (row) {
        printf("no more rows. last stripe:%d (id:%d) row:%d\n", stripe, data[stripe].id, row);
        data[stripe].rows = row;
        pthread_cond_signal(&data[stripe].cond);
        pthread_mutex_unlock(&data[stripe].lk);
        sleep(1);
    }

    printf("waiting for all thds to finish\n");
    for (int i = 0; i < THDS; ++i) {
        pthread_mutex_lock(&data[i].lk);
        finish = 1;
        pthread_cond_signal(&data[i].cond);
        pthread_mutex_unlock(&data[i].lk);
    }

    for (int i = 0; i < THDS; ++i) {
        pthread_cond_destroy(&data[i].cond);
        pthread_mutex_destroy(&data[i].lk);
        void *ret;
        pthread_join(data[i].thd, &ret);
        for (int j = 0; j < INSERTS; ++j) {
            for (int k = 0; k < 165; ++k) {
                free(data[i].val[j][k]);
            }
        }
        printf("thd:%d finished\n", i);
        cdb2_close(data[i].hndl);
    }

    PQfinish(pg);
    free(insert);
    free(data);
}
