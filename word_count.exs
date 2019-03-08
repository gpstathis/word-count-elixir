defmodule WordCount do
  @moduledoc """
  This program processes a file that contains people's names and outputs some
  counts as well as some transformed data.

  Input:

  An arbitrary file with in the following format:

  [...]
  Graham, Mckenna -- ut
      Voluptatem ipsam et at.
  Marvin, Garfield -- non
      Facere et necessitatibus animi.
  McLaughlin, Mariah -- consequatur
      Eveniet temporibus ducimus amet eaque.
  Lang, Agustina -- pariatur
      Unde voluptas sit fugit.
  [...]

  The program can be tested on much (e.g., 100x) larger files.

  Output:

  1. The unique count of full, last, and first names (i.e., duplicates are
     counted only once)
  2. The ten most common last names (the names and number of occurrences sorted
     in descending order)
  3. The ten most common first names (the names and number of occurrences
     sorted in descending order)
  4. A list of 25 modified names where no previous name has the same first name
     or same last name
  """
  @name_regex ~r/(?<full_name>(?<last>\w+), (?<first>\w+))/
  @top_count 10
  @modified_count 25

  @doc """
  Module entry point.
  """
  def run(data_file) do
    File.stream!(data_file)
    |> parse_lines
    |> compute_counts
    |> sort_counts(:first)
    |> sort_counts(:last)
    |> output_results
  end

  @doc """
  Extracts first and last names from valid lines, drops non valid lines.
  """
  def parse_lines(lines) do
    lines
    |> Stream.map(fn x -> Regex.named_captures(@name_regex, x) end)
    |> Stream.filter(fn x -> x != nil end)
  end

  @doc """
  Accumulates counts and distinct names into a map.
  """
  def compute_counts(data) do
    data
    |> Enum.reduce(
      %{
        :first => %{},
        :last => %{},
        :full => %{},
        :distinct_first => MapSet.new(),
        :distinct_last => MapSet.new()
      },
      fn x, acc ->
        increment(acc, :first, x["first"])
        |> increment(:last, x["last"])
        |> increment(:full, x["full_name"])
        |> gather_distinct_names(x)
      end
    )
  end

  @doc """
  Increments a name count.
  """
  def increment(acc, key, word) do
    update_in(acc, [key, Access.key(word, 0)], &(&1 + 1))
  end

  @doc """
  Gathers the first @modified_count distinct first and last names.
  """
  def gather_distinct_names(acc, names) do
    if MapSet.size(acc[:distinct_first]) < @modified_count and
         not MapSet.member?(acc[:distinct_first], names["first"]) and
         not MapSet.member?(acc[:distinct_last], names["last"]) do
      Map.put(acc, :distinct_first, MapSet.put(acc[:distinct_first], names["first"]))
      |> Map.put(:distinct_last, MapSet.put(acc[:distinct_last], names["last"]))
    else
      acc
    end
  end

  @doc """
  Converts a map of name counts to a tuple before reverse sorting by the
  tuple's second element.
  """
  def sort_counts(data, key) do
    Map.put(data, key, Map.to_list(data[key]))
    |> Map.put(key, Enum.sort_by(data[key], &elem(&1, 1), &>=/2))
  end

  def output_results(counts) do
    IO.puts("Unique first names: #{length(counts[:first])}")
    IO.puts("Unique last names: #{length(counts[:last])}")
    IO.puts("Unique full names: #{map_size(counts[:full])}")

    IO.puts("Top first names:")
    Enum.take(counts[:first], @top_count) |> IO.inspect()
    IO.puts("Top last names:")
    Enum.take(counts[:last], @top_count) |> IO.inspect()

    IO.puts("Top #{@modified_count} names:")

    Enum.zip(
      Enum.shuffle(MapSet.to_list(counts[:distinct_last])),
      Enum.shuffle(MapSet.to_list(counts[:distinct_first]))
    )
    |> IO.inspect()
  end
end

WordCount.run("./test-data.txt")
