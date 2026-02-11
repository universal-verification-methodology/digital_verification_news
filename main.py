import argparse
import logging
import os
import shutil
import sys
import time
from datetime import datetime
from typing import Any

import pytz

from utils import (
    back_up_files,
    generate_table,
    get_daily_date,
    get_daily_papers_by_keyword_with_retries,
    get_daily_papers_by_keyword_with_retries_acm,
    get_daily_papers_by_keyword_with_retries_crossref,
    get_daily_papers_by_keyword_with_retries_dvcon,
    get_daily_papers_by_keyword_with_retries_ieee,
    get_daily_papers_by_keyword_with_retries_openalex,
    get_daily_papers_by_keyword_with_retries_semantic_scholar,
    download_dvcon_assets,
    remove_backups,
    restore_files,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("ai_agriculture_news.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments for the paper fetcher.

    The arguments control which sources are queried and which keyword
    profile is used (for example, general CS or verification / UVM).
    """
    parser = argparse.ArgumentParser(
        description="Daily Papers Fetcher",
    )
    parser.add_argument(
        "--max-results",
        type=int,
        default=1000,
        help="Maximum number of query results from APIs for each keyword",
    )
    parser.add_argument(
        "--issues-results",
        type=int,
        default=200,
        help="Maximum number of papers to be included in the issue",
    )
    parser.add_argument(
        "--keywords",
        nargs="+",
        default=[
            "verification",
            "UVM",
        ],
        help="Keywords to search for papers",
    )
    parser.add_argument(
        "--profile",
        choices=["general", "verification"],
        default="general",
        help=(
            "Predefined topic profile. "
            "'general' (default) uses generic CS/ML-style keywords, while "
            "'verification' focuses on digital and UVM verification topics."
        ),
    )
    parser.add_argument(
        "--source",
        choices=[
            "arxiv",
            "crossref",
            "acm",
            "openalex",
            "semanticscholar",
            "ieee",
            "dvcon",
            "all",
        ],
        default="all",
        help=(
            "Primary data source to use. "
            "'arxiv' queries arXiv only, 'dvcon' queries only DVCon "
            "proceedings, and 'all' (default) combines arXiv with any enabled "
            "extra sources."
        ),
    )
    parser.add_argument(
        "--force-update",
        action="store_true",
        help="Force update even if already updated today",
    )
    parser.add_argument(
        "--include-crossref",
        action="store_true",
        help="Also fetch and include papers via CrossRef",
    )
    parser.add_argument(
        "--include-acm",
        action="store_true",
        help="Also fetch and include papers via the ACM Digital Library API",
    )
    parser.add_argument(
        "--include-openalex",
        action="store_true",
        help="Also fetch and include papers via OpenAlex",
    )
    parser.add_argument(
        "--include-dvcon",
        action="store_true",
        help="Also fetch and include DVCon-related entries via proceedings site",
    )
    parser.add_argument(
        "--include-ieee",
        action="store_true",
        help="Also fetch and include papers via IEEE Xplore keyword search",
    )
    parser.add_argument(
        "--include-semanticscholar",
        action="store_true",
        help="Also fetch and include papers via Semantic Scholar",
    )
    parser.add_argument(
        "--download-dvcon-assets",
        action="store_true",
        help="Download DVCon PDFs for any DVCon entries discovered",
    )
    return parser.parse_args()


def main() -> None:
    """Entry point for updating the daily papers README and issue template."""
    args = parse_arguments()

    # Apply keyword and source defaults for the verification / UVM profile.
    if getattr(args, "profile", "general") == "verification":
        logger.info(
            "Using 'verification' profile: focusing on digital/UVM verification topics.",
        )
        args.keywords = [
            "verification",
            "UVM",
        ]

        # If the caller has not explicitly enabled any optional sources,
        # turn them all on so the community gets a consolidated view.
        include_flags = [
            "include_crossref",
            "include_acm",
            "include_openalex",
            "include_dvcon",
            "include_ieee",
            "include_semanticscholar",
        ]
        if not any(getattr(args, flag, False) for flag in include_flags):
            logger.info(
                "No extra sources explicitly enabled; turning on CrossRef, ACM, "
                "OpenAlex, Semantic Scholar, IEEE and DVCon for consolidation.",
            )
            args.include_crossref = True
            args.include_acm = True
            args.include_openalex = True
            args.include_dvcon = True
            args.include_ieee = True
            args.include_semanticscholar = True

    # For the general profile (or any profile) when the caller has not
    # explicitly enabled extra sources and the primary source is "all",
    # turn on all optional providers so that the default run aggregates
    # everything.
    include_flags = [
        "include_crossref",
        "include_acm",
        "include_openalex",
        "include_dvcon",
        "include_ieee",
        "include_semanticscholar",
    ]
    if args.source == "all" and not any(getattr(args, flag, False) for flag in include_flags):
        logger.info(
            "Default source is 'all' with no extra sources explicitly enabled; "
            "turning on CrossRef, ACM, OpenAlex, Semantic Scholar, IEEE and "
            "DVCon for a consolidated run.",
        )
        args.include_crossref = True
        args.include_acm = True
        args.include_openalex = True
        args.include_dvcon = True
        args.include_ieee = True
        args.include_semanticscholar = True
    beijing_timezone = pytz.timezone("Asia/Singapore")
    current_date = datetime.now(beijing_timezone).strftime("%Y-%m-%d")

    logger.info("Starting Daily Papers Update Script")

    # Ensure .github directory exists
    os.makedirs(".github", exist_ok=True)

    # Check last update date
    try:
        with open("README.md") as f:
            while True:
                line = f.readline()
                if not line:
                    break
                if "Last update:" in line:
                    last_update_date = line.split(": ")[1].strip()
                    if last_update_date == current_date and not args.force_update:
                        logger.info(
                            "Already updated today! Use --force-update to override.",
                        )
                        return
                    break
    except FileNotFoundError:
        logger.info("README.md not found. Creating new file.")

    column_names = ["Title", "Link", "Abstract", "Date", "Comment"]

    try:
        back_up_files()
        logger.info("Backed up existing files")

        # write to README.md
        with open("README.md", "w") as f_rm:
            f_rm.write("# Daily Papers\n\n")
            f_rm.write("## Abstract\n")
            f_rm.write(
                "Daily Papers is an automated literature aggregation pipeline that "
                "collects, normalizes, and publishes up-to-date research digests for "
                "configurable topics. It queries arXiv and, optionally, CrossRef, "
                "OpenAlex, Semantic Scholar, IEEE Xplore, DVCon proceedings, and the "
                "ACM Digital Library, then consolidates the latest results into a "
                "single Markdown feed that is easy to browse and index by search "
                "engines.\n\n",
            )
            f_rm.write("## Overview\n")
            f_rm.write(
                "The project automatically fetches the latest papers from arXiv "
                "and optionally from CrossRef, OpenAlex, Semantic Scholar, IEEE, "
                "DVCon proceedings, and ACM Digital Library based on configurable "
                "keywords (for example, digital/UVM verification or other topics).\n\n",
            )
            f_rm.write(
                "The subheadings in the README file represent the search keywords "
                "(topics).\n\n",
            )
            f_rm.write(
                "Only the most recent articles for each keyword are "
                "retained, up to a maximum of 100 papers.\n\n",
            )
            f_rm.write(
                "You can click the 'Watch' button to receive daily email "
                "notifications.\n\n",
            )
            f_rm.write(f"Last update: {current_date}\n\n")

        # write to ISSUE_TEMPLATE.md
        with open(".github/ISSUE_TEMPLATE.md", "w") as f_is:
            f_is.write("---\n")
            f_is.write(
                f"title: Latest {args.issues_results} Papers - {get_daily_date()}\n",
            )
            f_is.write("labels: documentation\n")
            f_is.write("---\n")
            f_is.write(
                "**Please check the "
                "project's GitHub page for a better reading experience and more "
                "papers.**\n\n",
            )

        for keyword in args.keywords:
            logger.info("Processing keyword: %s", keyword)
            with open("README.md", "a") as f_rm, open(
                ".github/ISSUE_TEMPLATE.md",
                "a",
            ) as f_is:
                f_rm.write(f"## {keyword}\n")
                f_is.write(f"## {keyword}\n")

                link = "AND" if len(keyword.split()) == 1 else "OR"

                # arXiv papers (included when selected as primary or when combining all)
                if args.source in ["arxiv", "all"]:
                    papers = get_daily_papers_by_keyword_with_retries(
                        keyword,
                        column_names,
                        args.max_results,
                        link,
                    )
                    if papers is None:
                        raise Exception(f"Failed to get papers for keyword: {keyword}")

                    f_rm.write("### arXiv\n")
                    rm_table = generate_table(papers)
                    is_table = generate_table(
                        papers[: args.issues_results],
                        ignore_keys=["Abstract"],
                    )

                    f_rm.write(rm_table)
                    f_rm.write("\n\n")
                    f_is.write(is_table)
                    f_is.write("\n\n")

                    logger.info(
                        "Successfully processed %d arXiv papers for keyword: %s",
                        len(papers),
                        keyword,
                    )

                # CrossRef papers (optional)
                if args.include_crossref:
                    logger.info("Fetching CrossRef papers for keyword: %s", keyword)
                    cr_papers = get_daily_papers_by_keyword_with_retries_crossref(
                        keyword,
                        column_names,
                        args.max_results,
                    )
                    if cr_papers:
                        f_rm.write("### CrossRef\n")
                        rm_cr_table = generate_table(cr_papers)
                        is_cr_table = generate_table(
                            cr_papers[: args.issues_results],
                            ignore_keys=["Abstract"],
                        )
                        f_rm.write(rm_cr_table)
                        f_rm.write("\n\n")
                        f_is.write(is_cr_table)
                        f_is.write("\n\n")

                # ACM papers (optional, via official API)
                if args.include_acm:
                    logger.info("Fetching ACM papers for keyword: %s", keyword)
                    acm_papers = get_daily_papers_by_keyword_with_retries_acm(
                        keyword,
                        column_names,
                        args.max_results,
                    )
                    if acm_papers:
                        f_rm.write("### ACM (Digital Library API)\n")
                        rm_acm_table = generate_table(acm_papers)
                        is_acm_table = generate_table(
                            acm_papers[: args.issues_results],
                            ignore_keys=["Abstract"],
                        )
                        f_rm.write(rm_acm_table)
                        f_rm.write("\n\n")
                        f_is.write(is_acm_table)
                        f_is.write("\n\n")

                # OpenAlex papers (optional)
                if args.include_openalex:
                    logger.info("Fetching OpenAlex papers for keyword: %s", keyword)
                    oa_papers = get_daily_papers_by_keyword_with_retries_openalex(
                        keyword,
                        column_names,
                        args.max_results,
                    )
                    if oa_papers:
                        f_rm.write("### OpenAlex\n")
                        rm_oa_table = generate_table(oa_papers)
                        is_oa_table = generate_table(
                            oa_papers[: args.issues_results],
                            ignore_keys=["Abstract"],
                        )
                        f_rm.write(rm_oa_table)
                        f_rm.write("\n\n")
                        f_is.write(is_oa_table)
                        f_is.write("\n\n")

                # Semantic Scholar papers (optional)
                if args.include_semanticscholar:
                    logger.info(
                        "Fetching Semantic Scholar papers for keyword: %s",
                        keyword,
                    )
                    ss_papers = (
                        get_daily_papers_by_keyword_with_retries_semantic_scholar(
                            keyword,
                            column_names,
                            args.max_results,
                        )
                    )
                    if ss_papers:
                        f_rm.write("### Semantic Scholar\n")
                        rm_ss_table = generate_table(ss_papers)
                        is_ss_table = generate_table(
                            ss_papers[: args.issues_results],
                            ignore_keys=["Abstract"],
                        )
                        f_rm.write(rm_ss_table)
                        f_rm.write("\n\n")
                        f_is.write(is_ss_table)
                        f_is.write("\n\n")

                # IEEE papers (optional)
                if args.include_ieee:
                    logger.info("Fetching IEEE papers for keyword: %s", keyword)
                    ieee_papers = get_daily_papers_by_keyword_with_retries_ieee(
                        keyword,
                        column_names,
                        args.max_results,
                    )
                    if ieee_papers:
                        f_rm.write("### IEEE (Xplore)\n")
                        rm_ieee_table = generate_table(ieee_papers)
                        is_ieee_table = generate_table(
                            ieee_papers[: args.issues_results],
                            ignore_keys=["Abstract"],
                        )
                        f_rm.write(rm_ieee_table)
                        f_rm.write("\n\n")
                        f_is.write(is_ieee_table)
                        f_is.write("\n\n")

                # DVCon papers (optional, via CrossRef/OpenAlex venue filtering)
                if args.include_dvcon:
                    logger.info("Fetching DVCon papers for keyword: %s", keyword)
                    dvcon_papers = get_daily_papers_by_keyword_with_retries_dvcon(
                        keyword,
                        column_names,
                        args.max_results,
                    )
                    if dvcon_papers:
                        f_rm.write("### DVCon (proceedings archive)\n")
                        rm_dvcon_table = generate_table(dvcon_papers)
                        is_dvcon_table = generate_table(
                            dvcon_papers[: args.issues_results],
                            ignore_keys=["Abstract"],
                        )
                        f_rm.write(rm_dvcon_table)
                        f_rm.write("\n\n")
                        f_is.write(is_dvcon_table)
                        f_is.write("\n\n")

                        if args.download_dvcon_assets:
                            logger.info(
                                "Downloading DVCon assets for keyword: %s",
                                keyword,
                            )
                            download_dvcon_assets(dvcon_papers)

                time.sleep(5)  # avoid being blocked by remote APIs

        # Create dated archive in data folder
        data_dir = "data"
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)

        archive_filename = f"{current_date}.md"
        archive_path = os.path.join(data_dir, archive_filename)
        shutil.copy2("README.md", archive_path)
        logger.info("Created archive: %s", archive_path)

        remove_backups()
        logger.info("Script completed successfully!")

    except Exception as exc:  # noqa: BLE001
        logger.error("An error occurred: %s", exc)
        restore_files()
        raise


if __name__ == "__main__":
    main()
