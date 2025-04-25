import click

from datetime import datetime as dt
import os

from pycrossva.transform import transform, SUPPORTED_INPUTS, SUPPORTED_OUTPUTS


@click.command()
@click.argument("input_type",
                nargs=1,
                type=click.Choice(SUPPORTED_INPUTS + ['AUTODETECT']),
                required=True)
@click.argument('output_type',
                nargs=1,
                type=click.Choice(SUPPORTED_OUTPUTS),
                required=True)
@click.argument('src',
                nargs=-1,
                type=click.Path(),
                required=True)
@click.option("--column_id",
              help=("column name in the data file that contains the "
                    "unique ID for each death"),
              required=False)
@click.option('-d', '--dst', 'dst',
              help=("Specify destination filepath for transformed data "
                    "(default is current working directory)"),
              type=click.Path(),
              default=None,
              show_default="current directory",
              required=False)
@click.option('--silent', '-s', is_flag=True, default=False,
              help="Silence console output")
# @click.option('--preserve-na/--fill-na', default=False)
def main(input_type, output_type, src, column_id, dst, silent):
    """This is a wrapper for the `transform` python function in the pycrossva
    package so that it can be run from the command line. Once you have installed
    pycrossva, you can run this from the command line in order to process
    verbal autopsy data without having to touch python code. If you have multiple
    input files to process from the same input type (or source format) to the same
    output type (or algorithm), you run them all in a single command.

    If no destination (dst) is specified, the default behavior will be to write
    the resulting data to a csv in the current working directory with a name in
    the pattern of "output_type_from_src_mmddyy", where mmddyy is the current
    date. If `dst` is a directory, then the result file will still have the
    default name. If `dst` ends in '.csv' but multiple input files are given,
    then the output files will be written to dst_1.csv, dst_2.csv, etc.

    \b
    Args:
        `input_type` source type of the input data
        `output_type` format of output data (which algorithm the data should be prepared for)
        `src` filepath to the input data - can take multiple arguments, separated by a space

    \b
    Examples:
            $ pycrossva-transform 2012WHO InterVA4 path/to/mydata.csv
            2012WHO 'path/to/my/data.csv' data prepared for InterVA4 and written to csv at 'my/current/directory/InterVA4_from_mydata_042319.csv'

        \b
        $ pycrossva-transform 2012WHO InterVA4 path/to/mydata1.csv path/to/another/data2.csv --dst outputfolder
        2012WHO 'path/to/mydata1.csv' data prepared for InterVA4 and written to csv at 'outputfolder/InterVA4_from_mydata1_042319.csv'
        2012WHO 'path/to/another/data2.csv' data prepared for InterVA4 and written to csv at 'outputfolder/InterVA4_from_data2_042319.csv'

        \b
        $ pycrossva-transform 2012WHO InterVA4 path/to/mydata1.csv path/to/another/data2.csv --dst outputfolder/results.csv
        2012WHO 'path/to/mydata1.csv' data prepared for InterVA4 and written to csv at 'outputfolder/results_1.csv'
        2012WHO 'path/to/another/data2.csv' data prepared for InterVA4 and written to csv at 'outputfolder/results_2.csv'

    """
    verbosity = 2
    if silent:
        verbosity = 0
    for i in range(0, len(src)):
        input_file = src[i]

        # Automatically detect the input format if desired
        from pycrossva.utils import flexible_read, detect_format
        input_data = flexible_read(input_file)
        if input_type == "AUTODETECT":
            input_type = detect_format(output_type, input_data)
            click.echo(f"Detected input type: {input_type}")
        result = transform((input_type, output_type), input_data,
                           raw_data_id=column_id,
                           verbose=verbosity)
        original_name = input_file.split(os.path.sep)[-1].split(".")[0]
        default_name = "_".join([output_type, "from", original_name,
                                 dt.today().strftime("%m%d%y")]) + ".csv"
        if dst is not None:  # if dst is given
            # and it's a filename w/ entries
            if dst[-4:] == ".csv":
                final_dst = dst
                if len(src) > 1:
                    final_dst = final_dst.replace(
                        ".csv", "_" + str(i) + ".csv")
            else:
                final_dst = os.path.join(dst, default_name)
        else:
            final_dst = os.path.join(os.getcwd(), default_name)

        if len(src) > 1 and dst == final_dst:
            final_dst = dst + "_" + str(i + 1)

        if result is not None:
            result.to_csv(final_dst, index=False)
            if not silent:
                click.echo(
                    f"{input_type} '{input_file}' data prepared for {output_type} and written to csv at '{final_dst}'")


if __name__ == '__main__':
    main()
