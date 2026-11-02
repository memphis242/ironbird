/*
 * ironbird's TO_LAB subscription table for the cpu1 target. cFS takes this file instead of
 * sample_defs/tables/to_lab_sub.c because the cpu1_ prefix names it for that target; it carries
 * every stock entry and adds the ib_test generator's messages with a deep per-message limit.
 *
 * tools/cfs/build.sh copies this file into the checkout's sample_defs/tables/.
 */

/************************************************************************
 * NASA Docket No. GSC-18,719-1, and identified as “core Flight System: Bootes”
 *
 * Copyright (c) 2020 United States Government as represented by the
 * Administrator of the National Aeronautics and Space Administration.
 * All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ************************************************************************/

/**
 * \file
 *  Define TO Lab CPU specific subscription table
 */

#include "cfe_tbl_filedef.h" /* Required to obtain the CFE_TBL_FILEDEF macro definition */
#include "cfe_sb_api_typedefs.h"
#include "to_lab_tbl.h"

/*
** Add the proper include file for the message IDs below
*/

/*
** Common CFS app includes below are commented out
*/
#include "cfe_msgids.h"
#include "to_lab_msgids.h"
#include "ci_lab_msgids.h"

#include "sample_app_msgids.h"
#include "cf_msgids.h"
#include "hs_msgids.h"
#include "md_msgids.h"
#include "fm_msgids.h"
#include "lc_msgids.h"
#include "cs_msgids.h"
#include "mm_msgids.h"
#include "sc_msgids.h"
#include "ds_msgids.h"
#include "hk_msgids.h"

#ifdef CFS_SBN_ENABLED
#include "sbn_msgids.h"
#endif

#include "to_lab_internal_cfg.h"

/*
 * The generator's topic IDs (IB_TEST_GEN_TOPICID and IB_TEST_GEN_STATUS_TOPICID in
 * tools/cfs/apps/ib_test/fsw/src/ib_test.h), mapped the way ib_test maps them. The software bus
 * limits how many packets of one message may wait in TO_LAB's telemetry pipe to the subscription's
 * BufLimit, 255 at most through TO_LAB_ADD_PKT (about 25 ms of the generator's 10k packets/s) but
 * up to the pipe's depth through this table, so the pipe's depth alone bounds the stream.
 */
#define IRONBIRD_TO_LAB_GEN_TLM_MID        CFE_PLATFORM_TLM_TOPICID_TO_MIDV(0xE2)
#define IRONBIRD_TO_LAB_GEN_STATUS_TLM_MID CFE_PLATFORM_TLM_TOPICID_TO_MIDV(0xE3)
#define IRONBIRD_TO_LAB_GEN_BUF_LIMIT      1000

/*
 * TO_LAB's table validation accepts a BufLimit up to and including the pipe depth, and a limit
 * below the depth would again cap the stream before the pipe does.
 */
_Static_assert(IRONBIRD_TO_LAB_GEN_BUF_LIMIT == TO_LAB_PLATFORM_TLM_PIPE_DEPTH,
               "generator subscription limit must equal TO_LAB's telemetry pipe depth");
_Static_assert(IRONBIRD_TO_LAB_GEN_BUF_LIMIT <= 0xFFFF, "BufLimit is a uint16 in TO_LAB_Sub_t");
_Static_assert(IRONBIRD_TO_LAB_GEN_TLM_MID == 0x08E2 && IRONBIRD_TO_LAB_GEN_STATUS_TLM_MID == 0x08E3,
               "generator message IDs moved; update IB_TEST_GEN_TOPICID here and in ib_test.h");

/* clang-format: off */
TO_LAB_Subs_t TO_LAB_Subs =
{
    .Subs = {
        /* CFS App Subscriptions */
        {CFE_SB_MSGID_WRAP_VALUE(TO_LAB_HK_TLM_MID),          {0, 0}, 1},
        {CFE_SB_MSGID_WRAP_VALUE(TO_LAB_DATA_TYPES_MID),      {0, 0}, 1},
        {CFE_SB_MSGID_WRAP_VALUE(CI_LAB_HK_TLM_MID),          {0, 0}, 1},

        /* cFE Core subscriptions */
        {CFE_SB_MSGID_WRAP_VALUE(CFE_ES_HK_TLM_MID),          {0, 0}, 1},
        {CFE_SB_MSGID_WRAP_VALUE(CFE_EVS_HK_TLM_MID),         {0, 0}, 1},
        {CFE_SB_MSGID_WRAP_VALUE(CFE_SB_HK_TLM_MID),          {0, 0}, 1},
        {CFE_SB_MSGID_WRAP_VALUE(CFE_TBL_HK_TLM_MID),         {0, 0}, 1},
        {CFE_SB_MSGID_WRAP_VALUE(CFE_TIME_HK_TLM_MID),        {0, 0}, 1},
        {CFE_SB_MSGID_WRAP_VALUE(CFE_TIME_DIAG_TLM_MID),      {0, 0}, 1},
        {CFE_SB_MSGID_WRAP_VALUE(CFE_SB_STATS_TLM_MID),       {0, 0}, 1},
        {CFE_SB_MSGID_WRAP_VALUE(CFE_TBL_REG_TLM_MID),        {0, 0}, 1},
        {CFE_SB_MSGID_WRAP_VALUE(CFE_EVS_LONG_EVENT_MSG_MID), {0, 0}, 16},
        {CFE_SB_MSGID_WRAP_VALUE(CFE_ES_APP_TLM_MID),         {0, 0}, 1},
        {CFE_SB_MSGID_WRAP_VALUE(CFE_ES_MEMSTATS_TLM_MID),    {0, 0}, 1},

        /* Other CFS apps */
        {CFE_SB_MSGID_WRAP_VALUE(SAMPLE_APP_HK_TLM_MID), {0, 0}, 1},
        {CFE_SB_MSGID_WRAP_VALUE(CF_HK_TLM_MID),         {0, 0}, 1},
        {CFE_SB_MSGID_WRAP_VALUE(CF_EOT_TLM_MID),        {0, 0}, 1},
        {CFE_SB_MSGID_WRAP_VALUE(CF_CH0_TX_MID),        {0, 0}, 5},
        {CFE_SB_MSGID_WRAP_VALUE(CF_CH1_TX_MID),        {0, 0}, 5},
        {CFE_SB_MSGID_WRAP_VALUE(MD_HK_TLM_MID),         {0, 0}, 1},
        {CFE_SB_MSGID_WRAP_VALUE(HS_HK_TLM_MID),         {0, 0}, 1},
        {CFE_SB_MSGID_WRAP_VALUE(HK_HK_TLM_MID),         {0, 0}, 1},
        {CFE_SB_MSGID_WRAP_VALUE(MM_HK_TLM_MID),         {0, 0}, 1},
        {CFE_SB_MSGID_WRAP_VALUE(SC_HK_TLM_MID),         {0, 0}, 1},
        {CFE_SB_MSGID_WRAP_VALUE(FM_HK_TLM_MID),         {0, 0}, 1},
        {CFE_SB_MSGID_WRAP_VALUE(FM_FILE_INFO_TLM_MID),  {0, 0}, 1},
        {CFE_SB_MSGID_WRAP_VALUE(FM_DIR_LIST_TLM_MID),   {0, 0}, 1},
        {CFE_SB_MSGID_WRAP_VALUE(FM_OPEN_FILES_TLM_MID), {0, 0}, 1},
        {CFE_SB_MSGID_WRAP_VALUE(FM_MONITOR_TLM_MID),    {0, 0}, 1},
        {CFE_SB_MSGID_WRAP_VALUE(DS_HK_TLM_MID),         {0, 0}, 1},
        {CFE_SB_MSGID_WRAP_VALUE(LC_HK_TLM_MID),         {0, 0}, 1},
        {CFE_SB_MSGID_WRAP_VALUE(CS_HK_TLM_MID),         {0, 0}, 1},
#ifdef CFS_SBN_ENABLED
        {CFE_SB_MSGID_WRAP_VALUE(SBN_HK_TLM_MID),         {0, 0}, 1},
        {CFE_SB_MSGID_WRAP_VALUE(SBN_HKNET_TLM_MID),      {0, 0}, 1},
        {CFE_SB_MSGID_WRAP_VALUE(SBN_HKPEER_TLM_MID),     {0, 0}, 1},
        {CFE_SB_MSGID_WRAP_VALUE(SBN_HKMYSUBS_TLM_MID),   {0, 0}, 1},
        {CFE_SB_MSGID_WRAP_VALUE(SBN_HKPEERSUBS_TLM_MID), {0, 0}, 1},
#endif
        /* ironbird's ib_test generator (tools/cfs/apps/ib_test): the 10k packets/s stream and its
         * status packet, each allowed a full pipe's worth of queued packets. */
        {CFE_SB_MSGID_WRAP_VALUE(IRONBIRD_TO_LAB_GEN_TLM_MID),        {0, 0}, IRONBIRD_TO_LAB_GEN_BUF_LIMIT},
        {CFE_SB_MSGID_WRAP_VALUE(IRONBIRD_TO_LAB_GEN_STATUS_TLM_MID), {0, 0}, IRONBIRD_TO_LAB_GEN_BUF_LIMIT},

        /* TO_UNUSED entry to mark the end of valid MsgIds */
        {CFE_SB_MSGID_RESERVED, {0, 0}, 0}
    }
};

CFE_TBL_FILEDEF(TO_LAB_Subs, TO_LAB.Subscriptions, TO Lab Sub Tbl, to_lab_sub.tbl)
