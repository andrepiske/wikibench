
class PageRevision < Osto::Model
  col :id, :integer
  col :parentid, :integer
  col :timestamp, :timestamp
  col :sha1, :string
  col :text, :string
end

class WikiPage < Osto::Model
  col :title, :string
  col :id, :integer
  col :is_redirect, :boolean
  col :revisions, :array
end
