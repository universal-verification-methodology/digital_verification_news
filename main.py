import argparse
import logging
import os
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pytz

from utils import (
    back_up_files,
    download_dvcon_assets,
    extract_abstracts_from_downloaded_dvcon_pdfs,
    generate_table,
    get_daily_date,
    get_daily_papers_by_keyword_with_retries,
    get_daily_papers_by_keyword_with_retries_acm,
    get_daily_papers_by_keyword_with_retries_crossref,
    get_daily_papers_by_keyword_with_retries_dvcon,
    get_daily_papers_by_keyword_with_retries_ieee,
    get_daily_papers_by_keyword_with_retries_openalex,
    get_daily_papers_by_keyword_with_retries_semantic_scholar,
    remove_backups,
    restore_files,
    update_markdown_years_from_pdfs,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        # NOTE: This filename is mirrored in ``run.sh`` (LOG_FILE) so that
        # logs can be archived into ``logs/`` with a stable prefix.
        logging.FileHandler("daily_papers.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def specialise_keyword_for_source(
    base_keyword: str,
    source: str,
    profile: str,
) -> str:
    """Specialise a generic keyword for a given source and topic profile.

    For general discovery runs we keep the user-supplied keyword unchanged.
    For the ``verification`` profile, we tighten the query for broad digital
    libraries (arXiv, IEEE, ACM, CrossRef, OpenAlex, Semantic Scholar) by
    mapping a bare ``"verification"`` keyword to ``"digital verification"``.
    DVCon is already strongly scoped to hardware design and verification, so
    the broader ``"verification"`` keyword is preserved there.

    Args:
        base_keyword: The original keyword from the CLI or profile.
        source: Logical source name (e.g. ``"arxiv"``, ``"ieee"``, ``"dvcon"``).
        profile: Active topic profile (e.g. ``"general"`` or ``"verification"``).

    Returns:
        The possibly specialised keyword string to use for this source.
    """
    normalized_profile = profile.lower()
    normalized_source = source.lower()
    normalized_keyword = base_keyword.strip().lower()

    if normalized_profile != "verification":
        return base_keyword

    # If the user has already provided a specific phrase (e.g. contains
    # "uvm" or "digital"), respect it and do not rewrite.
    if "uvm" in normalized_keyword or "digital" in normalized_keyword:
        return base_keyword

    # For generic verification runs, tighten large digital libraries to
    # "digital verification" while keeping DVCon broad.
    scoped_sources = {
        "arxiv",
        "crossref",
        "acm",
        "openalex",
        "semanticscholar",
        "ieee",
    }
    if normalized_keyword == "verification" and normalized_source in scoped_sources:
        return "digital verification"

    return base_keyword


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
    """Entry point for updating the daily papers README and issue template.

    This function orchestrates the end-to-end update flow:

    * Parses CLI arguments and applies profile-specific defaults.
    * Backs up existing README and issue template files.
    * Queries the configured sources for each keyword.
    * Generates and writes markdown tables into ``README.md`` and
      ``.github/ISSUE_TEMPLATE.md``.
    * Archives the daily README snapshot into ``data/YYYY-MM-DD.md``.
    """
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

                # Start from the human-facing topic label, then specialise the
                # actual query per source where appropriate. The section
                # headings stay unchanged so that the README remains readable.
                #
                # NOTE: For verification-centric workflows we want DVCon and
                # other hardware-centric venues to be queried first, and leave
                # general-purpose aggregators like arXiv until last.
                link = "AND" if len(keyword.split()) == 1 else "OR"

                # DVCon papers (optional, via CrossRef/OpenAlex venue filtering
                # and dedicated proceedings scraper). This is prioritised so
                # that hardware/digital verification content appears first.
                if args.include_dvcon:
                    logger.info("Fetching DVCon papers for keyword: %s", keyword)
                    dvcon_keyword = specialise_keyword_for_source(
                        base_keyword=keyword,
                        source="dvcon",
                        profile=getattr(args, "profile", "general"),
                    )
                    dvcon_papers = get_daily_papers_by_keyword_with_retries_dvcon(
                        dvcon_keyword,
                        column_names,
                        args.max_results,
                    )
                    if dvcon_papers:
                        if args.download_dvcon_assets:
                            logger.info(
                                "Downloading DVCon assets for keyword: %s",
                                keyword,
                            )
                            download_dvcon_assets(dvcon_papers)
                            logger.info(
                                "Extracting abstracts from downloaded DVCon PDFs for keyword: %s",
                                keyword,
                            )
                            dvcon_papers = extract_abstracts_from_downloaded_dvcon_pdfs(
                                dvcon_papers,
                            )

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

                # IEEE papers (optional, but high priority for digital
                # verification content).
                if args.include_ieee:
                    logger.info("Fetching IEEE papers for keyword: %s", keyword)
                    ieee_keyword = specialise_keyword_for_source(
                        base_keyword=keyword,
                        source="ieee",
                        profile=getattr(args, "profile", "general"),
                    )
                    ieee_papers = get_daily_papers_by_keyword_with_retries_ieee(
                        ieee_keyword,
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

                # ACM papers (optional, via official API) – another
                # verification-heavy source that we prefer ahead of general
                # aggregators.
                if args.include_acm:
                    logger.info("Fetching ACM papers for keyword: %s", keyword)
                    acm_keyword = specialise_keyword_for_source(
                        base_keyword=keyword,
                        source="acm",
                        profile=getattr(args, "profile", "general"),
                    )
                    acm_papers = get_daily_papers_by_keyword_with_retries_acm(
                        acm_keyword,
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

                # CrossRef papers (optional)
                if args.include_crossref:
                    logger.info("Fetching CrossRef papers for keyword: %s", keyword)
                    crossref_keyword = specialise_keyword_for_source(
                        base_keyword=keyword,
                        source="crossref",
                        profile=getattr(args, "profile", "general"),
                    )
                    cr_papers = get_daily_papers_by_keyword_with_retries_crossref(
                        crossref_keyword,
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

                # OpenAlex papers (optional)
                if args.include_openalex:
                    logger.info("Fetching OpenAlex papers for keyword: %s", keyword)
                    openalex_keyword = specialise_keyword_for_source(
                        base_keyword=keyword,
                        source="openalex",
                        profile=getattr(args, "profile", "general"),
                    )
                    oa_papers = get_daily_papers_by_keyword_with_retries_openalex(
                        openalex_keyword,
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
                    semanticscholar_keyword = specialise_keyword_for_source(
                        base_keyword=keyword,
                        source="semanticscholar",
                        profile=getattr(args, "profile", "general"),
                    )
                    ss_papers = (
                        get_daily_papers_by_keyword_with_retries_semantic_scholar(
                            semanticscholar_keyword,
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

                # arXiv papers (included when selected as primary or when
                # combining all). This is placed last so that if it has to
                # retry (e.g. empty result sets) it does not delay the more
                # targeted DVCon/IEEE/ACM lookups.
                if args.source in ["arxiv", "all"]:
                    arxiv_keyword = specialise_keyword_for_source(
                        base_keyword=keyword,
                        source="arxiv",
                        profile=getattr(args, "profile", "general"),
                    )
                    link = "AND" if len(arxiv_keyword.split()) == 1 else "OR"
                    papers = get_daily_papers_by_keyword_with_retries(
                        arxiv_keyword,
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

                time.sleep(5)  # avoid being blocked by remote APIs

        # After generating the README, patch any DVCon rows that still carry
        # the legacy 1970 date placeholder by inferring years from local PDFs.
        try:
            update_markdown_years_from_pdfs(markdown_path=Path("README.md"))
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to post-process README dates from PDFs: %s", exc)

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
